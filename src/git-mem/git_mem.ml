module Simple = Mem
module Minor (Digestif : Digestif.S) = Minor.Make (Digestif)
module Major = Major
module Reference_store = Reference_store

let major_uid =
  {
    Git.Store.idx_major_uid_of_uid = (fun _ uid -> { Major.kind = `Idx; uid });
    pck_major_uid_of_uid = (fun _ uid -> { Major.kind = `Pack; uid });
    uid_of_major_uid = (fun { Major.uid; _ } -> uid);
  }
