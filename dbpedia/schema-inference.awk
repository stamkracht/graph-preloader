BEGIN {
  FS="> [<\"]";
}
{ # skip comments
  if (match($0, /^[ ]*#/)) {next;}

  if (match($3, /"@[a-zA-Z\-]+ \.$/)) {
    # language-coded literal
    range = substr($3, RSTART+1, RLENGTH-3);
  } else if (split($3, obj, /"\^\^</) > 1) {
    # typed literal
    range = substr(obj[2], 0, length(obj[2])-3);
  } else if (substr($3, length($3)-2, length($3)) == "> .") {
    if (match($3, /global\.dbpedia\.org\/id/)) {
      # DBpedia global URI
      range = "global_uri";
    } else {
      # external URI
      range = "external_uri";
    }
  } else {
    range = "string";
  }
  print substr($2, 1, length($2)), range;
}
# END {}
