// Decreases the number of likes for each of the listed documents.
let decrease = context.decreases[doc.rkey];
doc.likes = (doc.likes ?? 0) - decrease;
