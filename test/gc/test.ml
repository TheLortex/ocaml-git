let src = Logs.Src.create "git.mem" ~doc:"logs git-mem's events"

module Log = (val Logs.src_log src : Logs.LOG)
module Mn = Git_mem.Minor (Digestif.SHA1)
module Mj = Git_mem.Major
module Rs = Git_mem.Reference_store
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

let mk () =
  Store.v ~dotgit:(Fpath.v ".") ~minor:(Mn.v ()) ~major:(Mj.v ())
    ~major_uid:Git_mem.major_uid ~refs:(Rs.v ()) (Fpath.v ".")

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
