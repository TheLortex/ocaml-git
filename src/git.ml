(*
 * Copyright (c) 2013 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let todo feature =
  failwith (Printf.sprintf "TODO(%s)" feature)

module type ABSTRACT = sig
  type t
  val of_string: string -> t
  val to_string: t -> string
end

module type BLOCK = sig
  module ID : ABSTRACT
  module HEX: ABSTRACT
  type id  = ID.t
  type hex = HEX.t
  type t
  val dump: t -> unit
  val to_string: t -> string
  val of_string: file:string -> string -> t
end

module AbstractString = struct
  type t = string
  let of_string x = x
  let to_string x = x
end

module Base = struct
  module ID  = AbstractString
  module HEX = AbstractString
  type id  = ID.t
  type hex = HEX.t
end

let read_file filename =
  let ic = open_in_bin filename in
  let n = in_channel_length ic in
  let buf = String.create n in
  really_input ic buf 0 n;
  close_in ic;
  buf

let write_file filename ~contents =
  let oc = open_out_bin filename in
  output_string oc contents;
  close_out oc

module Zlib = struct

  let refil input =
    let n = String.length input in
    let toread = ref n in
    fun buf ->
      let m =
        if !toread <= String.length buf then !toread
        else String.length buf in
      String.blit input (n - !toread) buf 0 m;
      toread := !toread - m;
      m

  let flush output buf len =
    Buffer.add_substring output buf 0 len

  let deflate input =
    let output = Buffer.create 1024 in
    Zlib.compress (refil input) (flush output);
    Buffer.contents output

  let inflate input =
    let output = Buffer.create 1024 in
    Zlib.uncompress (refil input) (flush output);
    Buffer.contents output

end

let parse_error ~file fmt =
  Printf.kprintf (fun str ->
    Printf.eprintf
      "File %s\n\
       ---\n\
       %s\n\
       ---\n\
       Parse error: %s\n"
      file (Zlib.inflate (read_file file)) str;
    raise Parsing.Parse_error
  ) fmt

let sp  = '\x20'
let nul = '\x00'
let lf  = '\x0a'


(* ...|key value\n|...*)
(*    |   |     | |   *)
(*    |   i     j |   *)
(*   off         res  *)

(* ...|key value\0|...*)
(*    |   |     | |   *)
(*    |   i     j |   *)
(*   off         res  *)

type kv = {
  key  : string;
  value: string;
  sep  : char;
}

let string_sub orig buf off len =
  try String.sub buf off len
  with e ->
    Printf.printf "%s: buf=%s (%d) off=%d len=%d\n"
      orig buf (String.length buf) off len;
    raise e

let kv sep key value = { key; value; sep }

let input_kv sep ~file buf off =
  let i =
    try String.index_from buf off sp
    with Not_found -> parse_error ~file "invalid key." in
  let j =
    try String.index_from buf i sep
    with Not_found -> parse_error ~file "invalid value." in
  let key   = string_sub "kv.key"   buf off (i - off) in
  let value = string_sub "kv.value" buf (i + 1) (j - i - 1) in
  {key; value; sep}, j+1


let kv_nul = kv nul
let kv_lf  = kv lf

let input_kv_nul = input_kv nul
let input_kv_lf  = input_kv lf

let input_id ~file buf off =
  let n = 20 in
  if String.length buf - off >= n then
    string_sub "id" buf off n, off+20
  else
    parse_error ~file "invalid id."

let output_kv buf t =
  Buffer.add_string buf t.key;
  Buffer.add_char   buf sp;
  Buffer.add_string buf t.value;
  Buffer.add_char   buf t.sep

module User = struct

  type t = {
    name : string;
    email: string;
    date : string;
  }

  (* XXX needs to escape name/email/date *)
  let to_string t =
    Printf.sprintf "%s <%s> %s" t.name t.email t.date

  let of_string ~file s =
    let i =
      try String.index s '<'
      with Not_found -> parse_error ~file "invalid user name" in
    let j =
      try String.index_from s i '>'
      with Not_found -> parse_error ~file "invalide user email" in
    let name  = string_sub "user.name"  s 0 (i-1) in
    let email = string_sub "user.email" s (i + 1) (j - i - 1) in
    let date  = string_sub "user.date"  s (j + 2) (String.length s - j - 2) in
    { name; email; date }

  let pretty t =
    Printf.sprintf
      "[name : %s] [email: %s] [date : %s]"
      t.name t.email t.date

end

module Blob: BLOCK = struct

  include Base

  type t = string

  let dump x = Printf.printf "%s\n" x

  let to_string t = t

  let of_string ~file s = s

end

module rec Object: BLOCK = struct

  include Base

  type t =
    | Blob   of Blob.t
    | Commit of Commit.t
    | Tag    of Tag.t
    | Tree   of Tree.t

  let dump = function
    | Blob b ->
      Printf.printf "BLOB:\n";
      Blob.dump b
    | Commit c ->
      Printf.printf "COMMIT:\n";
      Commit.dump c
    | Tag t ->
      Printf.printf "TAG:\n";
      Tag.dump t
    | Tree t ->
      Printf.printf "TREE:\n";
      Tree.dump t

  let obj_type = function
    | Blob _   -> "blob"
    | Commit _ -> "commit"
    | Tag _    -> "tag"
    | Tree _   -> "tree"

  let contents = function
    | Blob b   -> Blob.to_string b
    | Commit c -> Commit.to_string c
    | Tag t    -> Tag.to_string t
    | Tree t   -> Tree.to_string t

  let to_string t =
    let value = contents t in
    let size = String.length value in
    let kv = kv_nul (obj_type t) (string_of_int size) in
    let buf = Buffer.create (2*size) in
    output_kv buf kv;
    Buffer.add_string buf value;
    Zlib.deflate (Buffer.contents buf)

  let of_string ~file s =
    let s = Zlib.inflate s in
    let kv, off = input_kv_nul ~file s 0 in
    let expected_size =
      try int_of_string kv.value
      with _ -> parse_error ~file "%s is not a valid integer." kv.value in
    let actual_size = String.length s - off in
    if expected_size <> actual_size then
      parse_error ~file
        "[expected: %d; actual: %d; start='%s']\n"
        expected_size actual_size
        (let k = min 10 (String.length s) in String.sub s off k);
    let contents = string_sub "object.contents" s off actual_size in
    match kv.key with
      | "blob"   -> Blob (Blob.of_string ~file contents)
      | "commit" -> Commit (Commit.of_string ~file contents)
      | "tag"    -> Tag (Tag.of_string ~file contents)
      | "tree"   -> Tree (Tree.of_string ~file contents)
      | x        -> parse_error ~file "%s is not a valid object type." x

end and Commit: BLOCK = struct

  include Base

  type t = {
    tree     : Tree.hex;
    parents  : hex list;
    author   : User.t;
    committer: User.t;
    message  : string;
  }

  let dump t =
    Printf.printf
      "  tree     : %s\n\
      \  parents  : %s\n\
      \  author   : %s\n\
      \  committer: %s\n\
      \  message  :\n%s\n"
      (Tree.HEX.to_string t.tree)
      (String.concat ", " (List.map ID.to_string t.parents))
      (User.pretty t.author)
      (User.pretty t.committer)
      t.message

  let to_string t =
    let tree = kv_lf "tree" (Tree.HEX.to_string t.tree) in
    let parent id = kv_lf "parent" (Tree.HEX.to_string t.tree) in
    let author = kv_lf "author" (User.to_string t.author) in
    let committer = kv_lf "committer" (User.to_string t.committer) in
    let buf = Buffer.create 1024 in
    output_kv buf tree;
    List.iter (fun id -> output_kv buf (parent id)) t.parents;
    output_kv buf author;
    output_kv buf committer;
    Buffer.add_char   buf lf;
    Buffer.add_string buf t.message;
    Buffer.contents buf

  let input_parents ~file s off =
    let rec aux off parents =
      let kv, noff = input_kv_lf ~file s off in
      if kv.key = "parent" then
        let id = ID.of_string kv.value in
        aux noff (id :: parents)
      else
        (List.rev parents, off) in
    aux off []

  let assert_key ~file actual expected =
    if actual <> expected then
      parse_error ~file "assert-keys: [actual: %s] [expected: %s]" actual expected

  let of_string ~file s =
    let tree, off = input_kv_lf ~file s 0 in
    assert_key ~file tree.key "tree";
    let parents, off = input_parents ~file s off in
    let author, off = input_kv_lf ~file s off in
    assert_key ~file author.key "author";
    let committer, off = input_kv_lf ~file s off in
    assert_key ~file committer.key "committer";
    let message = string_sub "commit.message" s (off + 1) (String.length s - off - 1) in
    {
      parents; message;
      tree      = Tree.HEX.of_string tree.value;
      author    = User.of_string ~file author.value;
      committer = User.of_string ~file committer.value;
    }

end and Tree: BLOCK = struct

  include Base

  type entry = {
    perm: [`normal|`exec|`link|`dir];
    name: string;
    id  : Object.id
  }
  type t = entry list

  let pretty_perm = function
    | `normal -> "normal"
    | `exec   -> "exec"
    | `link   -> "link"
    | `dir    -> "dir"

  let perm_of_string ~file = function
    | "100644" -> `normal
    | "100755" -> `exec
    | "120000" -> `normal
    | "40000"  -> `dir
    | x -> parse_error ~file "%s is not a valid permission." x

  let string_of_perm = function
    | `normal -> "100644"
    | `exec   -> "100755"
    | `link   -> "120000"
    | `dir    -> "40000"

  let dump_entry e =
    Printf.printf
      "perm: %s\n\
       name: %s\n\
       id  : %s\n"
      (pretty_perm e.perm)
      e.name
      (Object.ID.to_string e.id)

  let dump t =
    List.iter dump_entry t

  let to_string t = todo "tree"

  let of_string ~file buf =
    let rec entry off entries =
      if off >= String.length buf then
        List.rev entries
      else
        let kv, off = input_kv_nul ~file buf off in
        let id, off = input_id ~file buf off in
        let e = {
          perm = perm_of_string ~file kv.key;
          name = kv.value;
          id   = Object.ID.of_string id;
        } in
        entry off (e :: entries) in
    entry 0 []

end and Tag: BLOCK = struct

  include Base

  type t = {
    commit : Commit.hex;
    tag    : string;
    tagger : User.t;
    message: string;
  }

  let dump x = todo "tree"
  let to_string t = todo "tree"
  let of_string ~file s = todo "tree"

end

module Pack: BLOCK = struct

  include Base

  type object_id =
    | Blob   of Blob.id
    | Commit of Commit.id
    | Tag    of Tag.id
    | Tree   of Tree.id

  type delta = {
    base : object_id;
    delta: string;
  }

  type file =
    | Delta  of delta
    | Object of Object.t

  type t = {
    version: int;
    files  : file array;
  }

  let dump x = todo "pack"
  let to_string t = todo "pack"
  let of_string ~file s = todo "pack"

end

(*
type repository = {
  head   : string;
  index  : XXX;
  objects: Object.id list;
  refs   : refs;
}

type refs = {
  heads  : (string, commit) list;          (**  Local branches *)
  remotes: (string, commit) list;          (** Remote branches *)
}

*)
