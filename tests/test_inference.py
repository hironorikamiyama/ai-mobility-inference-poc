import pytest
import torch

from src.device import select_device
from src.inference import InferenceWorker


def test_cpu_device():
    assert select_device(prefer_gpu=False).type == "cpu"


def test_worker_cpu():
    worker = InferenceWorker(prefer_gpu=False)
    assert worker.device.type == "cpu"


def test_single_image():
    worker = InferenceWorker(prefer_gpu=False)

    image = torch.full((1, 1, 16, 16), 0.5)
    result = worker.predict(image)

    assert result == pytest.approx([0.5])


def test_multiple_images():
    worker = InferenceWorker(prefer_gpu=False)

    images = torch.stack([
        torch.full((1, 16, 16), 0.2),
        torch.full((1, 16, 16), 0.8),
    ])

    assert worker.predict(images) == pytest.approx([0.2, 0.8])


def test_invalid_shape():
    worker = InferenceWorker(prefer_gpu=False)

    with pytest.raises(ValueError):
        worker.predict(torch.zeros(1, 3, 16, 16))


def test_empty_batch():
    worker = InferenceWorker(prefer_gpu=False)

    assert worker.predict(torch.empty(0, 1, 16, 16)) == []
