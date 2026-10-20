import torch
from torch import nn

from src.device import select_device


class SimpleInferenceModel(nn.Module):
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.ndim != 4 or x.shape[1:] != (1, 16, 16):
            raise ValueError("入力は [batch, 1, 16, 16] の形状が必要です")

        return x.mean(dim=(1, 2, 3))


class InferenceWorker:
    def __init__(self, prefer_gpu: bool = True):
        self.device = select_device(prefer_gpu)
        self.model = SimpleInferenceModel().to(self.device)
        self.model.eval()

    def predict(self, images: torch.Tensor) -> list[float]:
        if images.ndim != 4 or images.shape[1:] != (1, 16, 16):
            raise ValueError("入力は [batch, 1, 16, 16] の形状が必要です")

        with torch.inference_mode():
            inputs = images.to(
                device=self.device,
                dtype=torch.float32,
            )
            scores = self.model(inputs)

        return scores.cpu().tolist()
