module Digestif = Digestif.SHA1

let src = Logs.Src.create "git-mem.major" ~doc:"logs git-mem's major heap"
module Log = (val Logs.src_log src : Logs.LOG)


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
    Bigstringaf.blit_from_string content ~src_off:0 buffer ~dst_off:current_size
      ~len:content_size;
    fd.content <- buffer;
    Art.insert t.data key buffer