let src = Logs.Src.create "git-mem.minor" ~doc:"logs git-mem's minor heap"

module Log = (val Logs.src_log src : Logs.LOG)

module Make (Digestif : Digestif.S) = struct
  module Index = Map.Make (struct
    type t = Digestif.t

    let compare = Digestif.unsafe_compare
  end)

  type t = {
    mutable size : int64;
    mutable data : Bigstringaf.t Art.t;
    max_size : int64 option;
  }

  let v ?max_size () = { data = Art.make (); size = 0L; max_size }

  type uid = Digestif.t
  type +'a fiber = 'a Lwt.t
  type error = string

  let pp_error = Fmt.string
  let key x = Digestif.to_hex x |> Art.key

  let exists { data; _ } uid =
    Log.debug (fun f -> f "Exists %a" Digestif.pp uid);
    let key = key uid in
    Lwt.return (Art.find_opt data key |> Option.is_some)

  let length { data; _ } uid =
    let key = key uid in
    match Art.find_opt data key with
    | None -> Lwt.return_error "not found"
    | Some value -> Lwt.return_ok (Int64.of_int (Bigstringaf.length value))

  let get_size { data; _ } uid =
    let key = key uid in
    match Art.find_opt data key with
    | None -> 0
    | Some value -> Bigstringaf.length value

  let map { data; _ } uid ~pos size =
    let key = key uid in
    match Art.find_opt data key with
    | None -> raise Not_found
    | Some data ->
        let slice = Bigstringaf.copy data ~off:(Int64.to_int pos) ~len:size in
        Lwt.return slice

  let concat s1 s2 =
    let l1 = Bigstringaf.length s1 in
    let l2 = Bigstringaf.length s2 in
    let buffer = Bigstringaf.create (l1 + l2) in
    Bigstringaf.blit s1 ~src_off:0 buffer ~dst_off:0 ~len:l1;
    Bigstringaf.blit s2 ~src_off:0 buffer ~dst_off:l1 ~len:l2;
    buffer

  let unsafe_append ~trunc t uid content =
    let content_len = Bigstringaf.length content in
    let key = key uid in
    let delta_size =
      match Art.find_opt t.data key with
      | None ->
          let content = Bigstringaf.copy content ~off:0 ~len:content_len in
          Art.insert t.data key content;
          content_len
      | Some old_content when trunc ->
          let content = Bigstringaf.copy content ~off:0 ~len:content_len in
          Art.insert t.data key content;
          content_len - Bigstringaf.length old_content
      | Some old_content ->
          let content = concat old_content content in
          Art.insert t.data key content;
          content_len
    in
    t.size <- Int64.add t.size (Int64.of_int delta_size)

  let append ({ size; max_size; _ } as t) uid content =
    let ( let* ) = Result.bind in
    let content_len = Bigstringaf.length content in
    let delta_size = content_len - get_size t uid in
    Log.debug (fun f ->
        f "Appendv %d (+%d) bytes -> %a" content_len delta_size Digestif.pp uid);
    Lwt.return
    @@ let* () =
         match max_size with
         | Some max_size
           when Int64.add size (Int64.of_int delta_size) > max_size ->
             Error `Out_of_memory
         | _ -> Ok ()
       in
       unsafe_append ~trunc:true t uid content;
       Ok ()

  let appendv ({ size; max_size; _ } as t) uid contents =
    let ( let* ) = Result.bind in
    let content_len =
      List.map Bigstringaf.length contents
      |> List.map Int64.of_int
      |> List.fold_left Int64.add Int64.zero
    in
    let current_size = get_size t uid |> Int64.of_int in
    let delta_size = Int64.sub content_len current_size in
    Log.debug (fun f ->
        f "Appendv %d (+%d) bytes -> %a" (Int64.to_int content_len)
          (Int64.to_int delta_size) Digestif.pp uid);
    Lwt.return
    @@ let* () =
         match max_size with
         | Some max_size when Int64.add size delta_size > max_size ->
             Error `Out_of_memory
         | _ -> Ok ()
       in
       List.iteri (fun i -> unsafe_append ~trunc:(i = 0) t uid) contents;
       Ok ()

  let list { data; _ } =
    Lwt.return
      (Art.to_seq data
      |> Seq.map (fun (key, _) ->
             let key_str = ((key :> Art.key) :> string) in
             Digestif.of_hex key_str)
      |> List.of_seq)

  let reset t =
    t.data <- Art.make ();
    t.size <- Int64.zero;
    Lwt.return_ok ()
end