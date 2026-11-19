(SkillCatalog "Chain observer: chain-status | chain-finalized-block | chain-query Rholang | chain-query-at block-hash Rholang")
(SkillHelp "chain" "Query the local F1R3node observer. chain-query selects the last finalized block post-state. chain-query-at uses an explicit full block hash. Put results on the first fresh unbound name, e.g. new return in { return!(42) }. Use a quoted heredoc for multiline source. Results retain typed RhoExpr values, the exact block and interpreter cost. No signing or persistent deploy submission is available.")
(SkillHelp "chain-query" "chain-query <<'RHO' followed by Rholang source and a closing RHO line. Use new return as the first unbound name and send results to return. Empty expr means no values were returned, not a failed HTTP request.")
(SkillContextHint "chain" "chain-status | chain-finalized-block | chain-query | chain-query-at")
(SkillHelp "chain-query-base64" "chain-query-base64 base64-utf8-source, or chain-query-at-base64 block-hash base64-utf8-source. Use these for exact bytes including literal _quote_ and _newline_ strings, which Hive's text command parser otherwise interprets as placeholders.")

