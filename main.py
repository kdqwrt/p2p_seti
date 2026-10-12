import argparse
import asyncio
import logging
from pathlib import Path

from p2p.config import ConfigError, NodeConfig
from p2p.node.node import Node


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Secure P2P overlay node"
    )

    parser.add_argument(
        "--config",
        required=True,
        help="Path to node configuration file",
    )

    return parser.parse_args()


def configure_logging(config: NodeConfig) -> None:
    """Настраивает вывод логов в консоль и файл."""

    log_path = Path(config.log_file)

    # Создаём папку logs, если её ещё нет.
    log_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    log_level = getattr(
        logging,
        config.log_level.upper(),
        logging.INFO,
    )

    formatter = logging.Formatter(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s "
        "%(message)s"
    )

    # Логи в терминал.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Те же логи в файл.
    file_handler = logging.FileHandler(
        log_path,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=log_level,
        handlers=[
            console_handler,
            file_handler,
        ],
    )


async def run_node(config: NodeConfig) -> None:
    node = Node(config)

    try:
        await node.start()
        await node.run()

    finally:
        await node.stop()


def main() -> None:
    args = parse_args()

    try:
        config = NodeConfig.from_file(
            args.config
        )

    except ConfigError as exc:
        raise SystemExit(
            f"Configuration error: {exc}"
        )

    # Настраиваем логирование ДО запуска Node.
    configure_logging(config)

    try:
        asyncio.run(
            run_node(config)
        )

    except KeyboardInterrupt:
        logging.info(
            "Node stopped by user"
        )


if __name__ == "__main__":
    main()