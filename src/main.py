import json

import torch

from src.inference import InferenceWorker


def main():
    worker = InferenceWorker()

    images = torch.stack([
        torch.full((1, 16, 16), 0.5),
        torch.full((1, 16, 16), 0.8),
    ])

    scores = worker.predict(images)

    print(json.dumps({
        "device": worker.device.type,
        "input_count": len(images),
        "scores": scores,
    }, indent=2))


if __name__ == "__main__":
    main()
