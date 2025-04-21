let context = doc.remove("context");
if context != () {
    // If there are no text and a context
    // it means that this document does not exist in the database
    // and the first operation we are trying to perform is a likes
    // modification, therefore, this document must be deleted.
    if doc.text == () {
        doc = ();
        return;
    }

    let addLikes = context.remove("addlikes");
    let removeLikes = context.remove("removeLikes");

    doc.likesIds = (doc.likesIds ?? []);
    doc.likes = (doc.likes ?? 0);

    if type_of(addLikes) == "array" {
        doc.likesIds += addLikes;
        doc.likes += addLikes.len;
    }

    if type_of(removeLikes) == "array" {
        doc.likesIds.retain(|id| id !in removeLikes);
        doc.likes -= removeLikes.len;
    }
}
