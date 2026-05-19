"""Example 5: PermanentFailure with partial-state cleanup.

A batch upload does partial work, then hits a quota. PermanentFailure carries
the already-uploaded IDs into the compensation task for cleanup.
"""

from _setup import app, backend  # noqa: F401

from celery_saga import PermanentFailure, Saga, SagaCompensated, StepResponse, saga_task


@app.task
def delete_uploaded(compensation_data):
    for fid in compensation_data["file_ids"]:
        print(f"  delete_uploaded: {fid}")


@saga_task(app, compensate=delete_uploaded)
def upload_batch(**kwargs):
    uploaded = []
    for file in kwargs["files"]:
        if file == "QUOTA_EXCEEDED":
            print(f"  upload_batch: quota hit after {len(uploaded)} files — failing")
            raise PermanentFailure(
                "Quota exceeded",
                compensation_data={"file_ids": uploaded},
            )
        print(f"  upload_batch: uploaded {file}")
        uploaded.append(f"f-{file}")
    return StepResponse(output={"file_ids": uploaded}, compensation_data={"file_ids": uploaded})


def main():
    saga = Saga("upload_saga").add_step(upload_batch)
    result = saga.run(files=["a.png", "b.png", "QUOTA_EXCEEDED", "c.png"])
    try:
        result.get(timeout=5)
    except SagaCompensated:
        print(f"  ✓ partial uploads cleaned up: status={result.status.value}")


if __name__ == "__main__":
    main()
