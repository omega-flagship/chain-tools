(SkillSignature chain-status)
(SkillSignature chain-finalized-block)
(SkillSignature chain-query (Arg multiline source))
(SkillSignature chain-query-at (Arg text block-hash) (Arg multiline source))
(SkillSignature chain-query-base64 (Arg base64-utf8 source))
(SkillSignature chain-query-at-base64 (Arg text block-hash) (Arg base64-utf8 source))
(SignatureLowering chain-query chain-query-base64)
(SignatureLowering chain-query-at chain-query-at-base64)

