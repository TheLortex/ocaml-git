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

module Minor = Git_mem.Minor (Digestif.SHA1)

module Store =
  Git.Store.Make (Digestif.SHA1) (Minor) (Git_mem.Major)
    (Git_mem.Reference_store)

module Test = Test_store.Make (Digestif.SHA1) (Store)

let store () =
  let store =
    let minor = Minor.v () in
    let major = Git_mem.Major.v () in
    let major_uid = Git_mem.major_uid in
    let refs = Git_mem.Reference_store.v () in
    Store.v ~dotgit:(Fpath.v "/empty") ~minor ~major ~major_uid ~refs
      (Fpath.v "/empty")
  in
  Lwt_main.run store

let store () = Cmdliner.Term.(const (store ()))
let () = Lwt_main.run (Test.test (store ()))
