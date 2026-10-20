import torch


def select_device(prefer_gpu: bool = True) -> torch.device:
    """CUDAが利用可能ならGPU、そうでなければCPUを選択する。"""
    if prefer_gpu and torch.cuda.is_available():
        return torch.device("cuda")

    return torch.device("cpu")
