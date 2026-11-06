"""Opt-in CPU probe using the installed, unmodified Transformers 5.17 callback."""

import json
import os
import socket
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


def main():
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root))
    sys.path.insert(1, os.environ["TRACKIO_PROBE_TRANSFORMERS_TARGET"])
    sys.path.extend(os.environ["TRACKIO_PROBE_FALLBACK_PATHS"].split(os.pathsep))

    import huggingface_hub
    import transformers
    from starlette.testclient import TestClient
    from transformers.integrations.integration_utils import TrackioCallback
    from transformers.trainer_callback import TrainerControl, TrainerState

    import trackio
    from trackio.cloud import CloudConfig, MetricsRepository, create_app
    from trackio.cloud_client import ManagedCloudRun

    assert transformers.__version__ == os.environ.get(
        "TRACKIO_PROBE_TRANSFORMERS_VERSION", "5.17.0"
    ), transformers.__version__
    assert huggingface_hub.__version__ == os.environ["TRACKIO_PROBE_HUB_VERSION"]

    def forbidden(*args, **kwargs):
        raise AssertionError("Native callback attempted upstream SQLite/HF/network")

    with tempfile.TemporaryDirectory(dir=root / ".ea") as temporary:
        directory = Path(temporary)
        repository = MetricsRepository(f"sqlite:///{directory / 'service.sqlite'}")
        settings = CloudConfig("local", repository.url, "s" * 32, "k" * 32)
        scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
        with TestClient(create_app(settings, repository)) as client:
            bound = client.post(
                "/v1/runs",
                json={**scope, "config": {}},
                headers={"Authorization": "Bearer " + "s" * 32},
            ).json()
            run = ManagedCloudRun(
                "http://testserver",
                bound["token"],
                "r",
                directory / "spool",
                transport=client,
            )
            args = SimpleNamespace(
                project="synthetic",
                run_name="display",
                trackio_space_id=None,
                trackio_bucket_id=None,
                trackio_static_space_id=None,
                hub_private_repo=True,
                to_dict=lambda: {"lr": 0.1, "api_key": "never-upload"},
            )
            model = SimpleNamespace(
                config={"model_type": "synthetic"}, num_parameters=lambda: 4
            )
            state, control = TrainerState(), TrainerControl()
            state.global_step = 3
            original_init = trackio.init

            def init_context(*args, **kwargs):
                with patch.object(trackio, "init_from_context", return_value=run):
                    return original_init(*args, **kwargs)

            with (
                patch.dict(
                    os.environ,
                    {"TRACKIO_ENVLOOP_CONTEXT_FILE": str(directory / "context")},
                ),
                patch.object(trackio, "init", side_effect=init_context),
                patch.object(
                    trackio.sqlite_storage.SQLiteStorage, "get_space_id", forbidden
                ),
                patch.object(
                    trackio.sqlite_storage.SQLiteStorage, "get_run_config", forbidden
                ),
                patch.object(huggingface_hub, "HfApi", forbidden),
                patch.object(socket, "create_connection", forbidden),
            ):
                callback = TrackioCallback()
                callback.on_train_begin(args, state, control, model=model)
                callback.on_log(args, state, control, model=model, logs={"loss": 0.5})
                callback.on_push_begin(args, state, control, model=model)
                assert callback._static_space_id is None
                assert not getattr(model, "model_tags", None)
                callback.on_train_end(args, state, control, model=model)
                assert run.delivery_status["committed"]
                assert trackio.sync(project="synthetic", sdk="static") is None
                assert trackio.freeze("example/forbidden", "synthetic") is None
            rows = client.get(
                "/v1/metrics",
                params=scope,
                headers={"Authorization": "Bearer " + "s" * 32},
            ).json()
            assert "never-upload" not in json.dumps(rows)
            assert repository.events("w", "p", "j")[0]["metrics"]["train/loss"] == 0.5
        repository.close()
    print(
        json.dumps(
            {
                "trackio": trackio.__version__,
                "transformers": transformers.__version__,
                "huggingface_hub": huggingface_hub.__version__,
                "callback_file": sys.modules[TrackioCallback.__module__].__file__,
                "passed": True,
            }
        )
    )


if __name__ == "__main__":
    main()
