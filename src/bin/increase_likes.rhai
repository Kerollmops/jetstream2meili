// Increases the number of likes for each of the listed documents.
let increase = context.increases[doc.rkey];
doc.likes = (doc.likes ?? 0) + increase;
