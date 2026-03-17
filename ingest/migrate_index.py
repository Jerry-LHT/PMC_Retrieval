from __future__ import annotations

import argparse

from app.config import get_settings
from search.opensearch_client import OpenSearchGateway


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create/reindex/switch OpenSearch versioned index")
    parser.add_argument("--config", default="config/app.yaml", help="Path to YAML config")
    parser.add_argument(
        "--target-index",
        default=None,
        help="Target index name (defaults to settings.opensearch.index_name)",
    )
    parser.add_argument(
        "--source-index",
        default=None,
        help="Source index or alias to reindex from (defaults to settings.opensearch.index_alias)",
    )
    parser.add_argument("--mapping-path", default="search/mapping.json", help="Index mapping file path")
    parser.add_argument(
        "--skip-reindex",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Only create target index; skip reindex",
    )
    parser.add_argument(
        "--switch-alias",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Switch alias to target index after reindex",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=3600,
        help="Reindex request timeout in seconds",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = get_settings(args.config)
    gateway = OpenSearchGateway(settings)
    gateway.wait_until_ready()

    target_index = args.target_index or gateway.index_name
    source_index = args.source_index or gateway.index_alias

    gateway.create_index_if_missing(target_index, mapping_path=args.mapping_path)

    reindex_response: dict | None = None
    if not args.skip_reindex and source_index != target_index:
        reindex_response = gateway.reindex(
            source_index=source_index,
            target_index=target_index,
            wait_for_completion=True,
            request_timeout=args.request_timeout,
        )

    if args.switch_alias:
        gateway.switch_alias_to_index(target_index)

    print(
        f"target_index={target_index} "
        f"source_index={source_index} "
        f"reindexed={'yes' if reindex_response is not None else 'no'} "
        f"alias_switched={'yes' if args.switch_alias else 'no'}"
    )


if __name__ == "__main__":
    main()
