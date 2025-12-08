from dataclasses import dataclass
from typing import Any

SnapshotData = dict[str, dict]
Blob = dict


class SnapshotDiff:
    from_snapshot: str
    to_snapshot: str
    changed_keys: list[str]


@dataclass
class ConfigBlob:
    data: dict
    frozen: bool = False


class ConfigSnapshot:
    def __init__(self, data: SnapshotData) -> None:
        self.data = data

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigSnapshot(...)")
        else:
            with p.group(4, "ConfigSnapshot(", ")"):
                p.breakable()
                p.text(f"data={self.data},")
                p.breakable()

    def get(self, key: str) -> dict | None:
        return self.data.get(key)


class ConfigStage:
    def __init__(self, snapshot: ConfigSnapshot) -> None:
        self.snapshot = snapshot
        self.data: dict[str, dict] = {}

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigStage(...)")
        else:
            with p.group(4, "ConfigStage(", ")"):
                p.breakable()
                p.text("snapshot=")
                p.pretty(self.snapshot)
                p.text(",")
                p.breakable()
                p.text("data=")
                p.pretty(self.data)
                p.breakable()

    def get(self, key: str) -> dict | None:
        if key in self.data:
            return self.data[key]
        else:
            return self.snapshot.get(key)

    def set(self, key: str, value: dict | None) -> None:
        if value is None:
            if key in self.data:
                del self.data[key]
        else:
            self.data[key] = value

    def freeze(self) -> ConfigSnapshot:
        return ConfigSnapshot(
            {
                **self.snapshot.data,
                **self.data,
            }
        )


class ConfigRepo:
    @classmethod
    def create(cls) -> "ConfigRepo":
        return cls()

    def __init__(self) -> None:
        self.repo: dict[str, ConfigSnapshot] = {
            "master": ConfigSnapshot({}),
        }

        self.base = self.repo["master"]
        self.stage = ConfigStage(self.base)

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigRepo(...)")
        else:
            with p.group(4, "ConfigRepo(", ")"):
                p.breakable()
                p.text("base=")
                p.pretty(self.base)
                p.text(",")
                p.breakable()
                p.text("stage=")
                p.pretty(self.stage)
                p.text(",")
                p.breakable()

    def commit(self) -> None:
        new_base = self.stage.freeze()
        self.repo["master"] = new_base
        self.base = new_base

        self.stage = ConfigStage(self.base)

    def switch_branch_and_pull(self, branch: str) -> SnapshotDiff:
        raise NotImplementedError()


def main():
    configs = ConfigRepo.create()

    configs.stage.set("example", {"key": "value"})
    configs.commit()


if __name__ == "__main__":
    main()
