let src = Logs.Src.create "git.mem" ~doc:"logs git-mem's events"

module Log = (val Logs.src_log src : Logs.LOG)

module Mn = struct
  module Digestif = Digestif.SHA1

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
    (* Log.debug (fun f -> f "Length %a" Digestif.pp uid);*)
    let key = key uid in
    match Art.find_opt data key with
    | None -> Lwt.return_error "not found"
    | Some value ->
        (*   Log.debug (fun f -> f "...=> %d" (Bigstringaf.length value));*)
        Lwt.return_ok (Int64.of_int (Bigstringaf.length value))

  let get_size { data; _ } uid =
    let key = key uid in
    match Art.find_opt data key with
    | None -> 0
    | Some value -> Bigstringaf.length value

  let map { data; _ } uid ~pos size =
    (* Log.debug (fun f ->
         f "Read %a (%d, %d)" Digestif.pp uid (Int64.to_int pos) size);*)
    let key = key uid in
    match Art.find_opt data key with
    | None -> raise Not_found
    | Some data ->
        (*  Log.debug (fun f -> f "...=> %d" (Bigstringaf.length data));*)
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

module Mj = struct
  module Digestif = Digestif.SHA1

  type 'a rd = < rd : unit ; .. > as 'a
  type 'a wr = < wr : unit ; .. > as 'a

  type 'a mode =
    | Rd : < rd : unit > mode
    | Wr : < wr : unit > mode
    | RdWr : < rd : unit ; wr : unit > mode

  type +'a fiber = 'a Lwt.t
  type uid = { kind : [ `Idx | `Pack ]; uid : Digestif.t }

  type 'a fd = {
    uid : uid;
    mutable trunc : bool;
    mutable content : Bigstringaf.t;
  }

  type t = { mutable data : Bigstringaf.t Art.t }

  let v () = { data = Art.make () }

  type error = string

  let pp_error = Fmt.string

  let reset t =
    t.data <- Art.make ();
    Lwt.return_ok ()

  let art_of_key { kind; uid } =
    let prefix = match kind with `Idx -> "idx" | `Pack -> "pack" in
    prefix ^ "-" ^ Digestif.to_hex uid |> Art.key

  let key_of_art art =
    let key_str = ((art :> Art.key) :> string) in
    match String.split_on_char '-' key_str with
    | [ "idx"; uid ] -> { uid = Digestif.of_hex uid; kind = `Idx }
    | [ "pack"; uid ] -> { uid = Digestif.of_hex uid; kind = `Pack }
    | _ -> failwith "Unexpected key format"

  let create ?(trunc = true) ~mode:_ t uid =
    let key = art_of_key uid in
    match Art.find_opt t.data key with
    | None -> Lwt.return_ok { trunc; uid; content = Bigstringaf.empty }
    | Some content -> Lwt.return_ok { trunc; uid; content }

  let map _t fd ~pos len =
    Bigstringaf.copy fd.content ~off:(Int64.to_int pos) ~len

  let close _ _ = Lwt.return_ok ()

  let list t =
    Lwt.return
      (Art.to_seq t.data
      |> Seq.map (fun (key, _) -> key_of_art key)
      |> List.of_seq)

  let length { content; _ } =
    Bigstringaf.length content |> Int64.of_int |> Lwt.return

  let move t ~src ~dst =
    let src_key = art_of_key src in
    let dst_key = art_of_key dst in
    match Art.find_opt t.data src_key with
    | None -> Lwt.return_error "source not found"
    | Some source_ba ->
        Art.remove t.data src_key;
        Art.insert t.data dst_key source_ba;
        Lwt.return_ok ()

  let append t fd content =
    let key = art_of_key fd.uid in
    Lwt.return
    @@
    if fd.trunc then (
      let content =
        Bigstringaf.of_string ~off:0 ~len:(String.length content) content
      in
      fd.trunc <- false;
      fd.content <- content;
      Art.insert t.data key content)
    else
      let current_size = Bigstringaf.length fd.content in
      let content_size = String.length content in
      let new_size = current_size + content_size in
      let buffer = Bigstringaf.create new_size in
      Bigstringaf.blit fd.content ~src_off:0 buffer ~dst_off:0 ~len:current_size;
      Bigstringaf.blit_from_string content ~src_off:0 buffer
        ~dst_off:current_size ~len:content_size;
      fd.content <- buffer;
      Art.insert t.data key buffer
end

module Rs = struct
  module Map = Git.Reference.Map

  type +'a fiber = 'a
  type t = string Map.t ref

  let v () = ref Map.empty

  type error = string

  let pp_error = Fmt.string

  let atomic_wr t ref value =
    t := Map.add ref value !t;
    Ok ()

  let atomic_rd t ref = Ok (Map.find ref !t)

  let atomic_rm t ref =
    t := Map.remove ref !t;
    Ok ()

  let list t = Map.bindings !t |> List.map fst

  let reset t =
    t := Map.empty;
    Ok ()
end

module Store = Git.Store.Make (Digestif.SHA1) (Mn) (Mj) (Rs)
module Test = Test_store.Make (Digestif.SHA1) (Store)
open Cmdliner

let reporter ppf =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let with_metadata header _tags k ppf fmt =
      Format.kfprintf k ppf
        ("%a[%a]: " ^^ fmt ^^ "\n%!")
        Logs_fmt.pp_header (level, header)
        Fmt.(styled `Magenta string)
        (Logs.Src.name src)
    in
    msgf @@ fun ?header ?tags fmt -> with_metadata header tags k ppf fmt
  in
  { Logs.report }

let () = Fmt_tty.setup_std_outputs ~style_renderer:`Ansi_tty ~utf_8:true ()
let () = Logs.set_reporter (reporter Fmt.stderr)
let () = Logs.set_level ~all:true (Some Logs.Debug)

let major_uid =
  {
    Git.Store.idx_major_uid_of_uid = (fun _ uid -> { Mj.kind = `Idx; uid });
    pck_major_uid_of_uid = (fun _ uid -> { Mj.kind = `Pack; uid });
    uid_of_major_uid = (fun { Mj.uid; _ } -> uid);
  }

let mk () =
  Store.v ~dotgit:(Fpath.v ".") ~minor:(Mn.v ()) ~major:(Mj.v ()) ~major_uid
    ~refs:(Rs.v ()) (Fpath.v ".")

let store : Store.t = Lwt_main.run @@ mk ()

let author =
  { Git.User.name = "test"; email = "test@test.com"; date = 0L, None }

let run () =
  let open Lwt.Syntax in
  let ( let** ) = Lwt_result.bind in
  let* () = Test.test (Term.const store) in
  let* store = mk () in
  let** blob_1, _ =
    Store.write store Store.Value.(blob (Blob.of_string "blob 1"))
  in
  let** blob_2, _ =
    Store.write store Store.Value.(blob (Blob.of_string "blob 2 "))
  in
  let** blob_3, _ =
    Store.write store Store.Value.(blob (Blob.of_string "blob 3  "))
  in
  let** blob_4, _ =
    Store.write store Store.Value.(blob (Blob.of_string "blob 4   "))
  in
  let** root_tree_hash, _ =
    Store.write store
      Store.Value.(
        tree
          (Tree.v
             [
               { name = "blob1"; node = blob_1; perm = `Normal };
               { name = "blob2"; node = blob_2; perm = `Normal };
             ]))
  in
  let** commit_hash, _ =
    Store.write store
      Store.Value.(
        commit
          (Commit.make ~tree:root_tree_hash ~author ~committer:author
             (Some "root commit")))
  in
  let** () =
    Store.Ref.write store
      (Git.Reference.v "heads/main")
      (Git.Reference.Uid commit_hash)
  in
  let* _ = Store.read_exn store blob_1 in
  let* _ = Store.read_exn store blob_2 in
  let* _ = Store.read_exn store blob_3 in
  let* _ = Store.read_exn store blob_4 in
  let* () = Store.gc store in
  let* v = Store.read_exn store blob_1 in
  (match v with
  | Git.Value.Blob v when Store.Value.Blob.to_string v = "blob 1" -> ()
  | _ -> failwith "no");
  let* _ = Store.read_exn store blob_2 in
  let* () = Store.gc store in

  Lwt.return_ok ()

let () = Lwt_main.run (run () |> Lwt.map Result.get_ok)
