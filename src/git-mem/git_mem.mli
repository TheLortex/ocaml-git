module Simple = Mem

module Minor (D : Digestif.S) : sig
  include Loose_git.STORE with type +'a fiber = 'a Lwt.t and type uid = D.t

  val v : ?max_size:int64 -> unit -> t
end

module Major : sig
  include Git.Store.Mj with type +'a fiber = 'a Lwt.t

  val v : unit -> t
end

module Reference_store : sig
  include Git.Store.Rs with type +'a fiber = 'a

  val v : unit -> t
end

val major_uid : (Digestif.SHA1.t, Major.uid, Major.t) Git.Store.major
