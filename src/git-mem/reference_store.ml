module Map = Git.Reference.Map

let src = Logs.Src.create "git-mem.refstore" ~doc:"logs git-mem's reference store"

module Log = (val Logs.src_log src : Logs.LOG)

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