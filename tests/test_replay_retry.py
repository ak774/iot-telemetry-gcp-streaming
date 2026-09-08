import pytest

from google.api_core import exceptions as core_exceptions

from dataflow.transforms.replay_repository import (
    ReplayRegistryRepository,
)


def make_repository():
    return ReplayRegistryRepository(
        project_id="iot-gcp-streaming",
        instance_id="iot-streaming",
        database_id="iot_registry",
    )


def test_retryable_failure_then_success(monkeypatch):
    repository = make_repository()

    calls = []

    def fake_operation():
        calls.append(1)

        if len(calls) == 1:
            raise core_exceptions.ServiceUnavailable(
                "simulated transient failure"
            )

        return "success"

    monkeypatch.setattr(
        "dataflow.transforms.replay_repository.time.sleep",
        lambda _: None,
    )

    result = repository._run_with_retry(
        fake_operation,
        "test_operation",
    )

    assert result == "success"
    assert len(calls) == 2


def test_retryable_failure_exhausts_attempts(monkeypatch):
    repository = make_repository()

    calls = []

    def fake_operation():
        calls.append(1)

        raise core_exceptions.ServiceUnavailable(
            "simulated persistent transient failure"
        )

    monkeypatch.setattr(
        "dataflow.transforms.replay_repository.time.sleep",
        lambda _: None,
    )

    with pytest.raises(core_exceptions.ServiceUnavailable):
        repository._run_with_retry(
            fake_operation,
            "test_operation",
        )

    assert len(calls) == 3


def test_permanent_failure_is_not_retried(monkeypatch):
    repository = make_repository()

    calls = []

    def fake_operation():
        calls.append(1)

        raise core_exceptions.PermissionDenied(
            "simulated permission failure"
        )

    monkeypatch.setattr(
        "dataflow.transforms.replay_repository.time.sleep",
        lambda _: None,
    )

    with pytest.raises(core_exceptions.PermissionDenied):
        repository._run_with_retry(
            fake_operation,
            "test_operation",
        )

    assert len(calls) == 1


def test_already_exists_is_not_retried(monkeypatch):
    repository = make_repository()

    calls = []

    def fake_operation():
        calls.append(1)

        raise core_exceptions.AlreadyExists(
            "simulated duplicate insert"
        )

    monkeypatch.setattr(
        "dataflow.transforms.replay_repository.time.sleep",
        lambda _: None,
    )

    with pytest.raises(core_exceptions.AlreadyExists):
        repository._run_with_retry(
            fake_operation,
            "test_operation",
        )

    assert len(calls) == 1