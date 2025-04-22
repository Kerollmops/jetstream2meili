let context = doc.remove("context");
if context != () {
    // If there are no text and a context
    // it means that this document does not exist in the database
    // and the first operation we are trying to perform is a likes
    // modification, therefore, this document must be deleted.
    if "text" !in doc {
        doc = ();
        return;
    }

    let addLikes = context.remove("addlikes");
    doc.likes = (doc.likes ?? 0);
    doc.likes += addLikes;
}
