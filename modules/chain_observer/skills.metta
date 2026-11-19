(= (chain-status) (py-call (chain_observer.observer_status)))
(= (chain-finalized-block) (py-call (chain_observer.observer_finalized_block)))
(= (chain-query $source) (py-call (chain_observer.query_state $source)))
(= (chain-query-at $block-hash $source) (py-call (chain_observer.query_state_at $block-hash $source)))
(= (chain-query-base64 $source) (py-call (chain_observer.query_state_base64 $source)))
(= (chain-query-at-base64 $block-hash $source) (py-call (chain_observer.query_state_at_base64 $block-hash $source)))

