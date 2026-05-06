from datetime import datetime

from models.entities import WbToken
from services.background_sync import claim_next_sync_job, create_sync_job, find_retryable_sync_job, get_latest_sync_status


def test_auto_sync_reuses_active_job(db):
    token = WbToken(
        user_id=1,
        encrypted_token="stub",
        token_status="active",
        is_active=True,
        sync_in_progress=False,
    )
    db.add(token)
    db.commit()
    db.refresh(token)

    first = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="auto_sync")
    second = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="auto_sync")

    assert first.id == second.id

    db.refresh(token)
    assert token.sync_in_progress is True


def test_worker_claims_queued_job(db):
    token = WbToken(
        user_id=1,
        encrypted_token="stub",
        token_status="active",
        is_active=True,
        sync_in_progress=False,
    )
    db.add(token)
    db.commit()
    db.refresh(token)

    job = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="manual_sync")
    claimed = claim_next_sync_job(db)

    assert claimed == (job.id, 1)


def test_retry_partial_reuses_completed_steps(db):
    token = WbToken(
        user_id=1,
        encrypted_token="stub",
        token_status="active",
        is_active=True,
        sync_in_progress=False,
    )
    db.add(token)
    db.commit()
    db.refresh(token)

    original = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="manual_sync")
    step_map = {step.step_name: step for step in original.steps}
    finished_at = datetime(2026, 5, 5, 12, 0, 0)

    step_map["products"].status = "completed"
    step_map["products"].records_received = 168
    step_map["products"].records_saved = 168
    step_map["products"].started_at = finished_at
    step_map["products"].finished_at = finished_at

    step_map["stocks"].status = "completed"
    step_map["stocks"].records_received = 168
    step_map["stocks"].records_saved = 168
    step_map["stocks"].started_at = finished_at
    step_map["stocks"].finished_at = finished_at

    step_map["sales"].status = "skipped"
    step_map["sales"].error_message = "rate limited"
    step_map["sales"].finished_at = finished_at

    original.status = "partial"
    original.progress_percent = 25
    original.finished_at = finished_at
    db.commit()
    db.refresh(original)

    retry = create_sync_job(
        db,
        user_id=1,
        wb_token_id=token.id,
        sync_type="retry_partial",
        retry_from=original,
    )

    retry_steps = {step.step_name: step for step in retry.steps}

    assert retry.progress_percent == 25
    assert retry_steps["products"].status == "completed"
    assert retry_steps["products"].records_saved == 168
    assert retry_steps["stocks"].status == "completed"
    assert retry_steps["sales"].status == "pending"
    assert retry_steps["token_check"].status == "pending"
    assert retry_steps["dashboard_calc"].status == "pending"


def test_finished_partial_job_does_not_block_auto_sync(db):
    token = WbToken(
        user_id=1,
        encrypted_token="stub",
        token_status="active",
        is_active=True,
        sync_in_progress=False,
    )
    db.add(token)
    db.commit()
    db.refresh(token)

    partial = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="manual_sync")
    partial.status = "partial"
    partial.started_at = datetime(2026, 5, 5, 12, 0, 0)
    partial.finished_at = datetime(2026, 5, 5, 12, 10, 0)
    token.sync_in_progress = False
    db.commit()
    db.refresh(partial)

    auto_job = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="auto_sync")

    assert auto_job.id != partial.id
    assert auto_job.status == "queued"


def test_find_retryable_sync_job_requires_required_data_step(db):
    token = WbToken(
        user_id=1,
        encrypted_token="stub",
        token_status="active",
        is_active=True,
        sync_in_progress=False,
    )
    db.add(token)
    db.commit()
    db.refresh(token)

    partial = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="manual_sync")
    step_map = {step.step_name: step for step in partial.steps}
    finished_at = datetime(2026, 5, 5, 12, 0, 0)
    step_map["products"].status = "skipped"
    step_map["stocks"].status = "skipped"
    partial.status = "partial"
    partial.finished_at = finished_at
    token.sync_in_progress = False
    db.commit()

    assert find_retryable_sync_job(db, 1, token.id).id == partial.id

    step_map["stocks"].status = "completed"
    db.commit()

    assert find_retryable_sync_job(db, 1, token.id) is None


def test_latest_status_repairs_finished_queued_job(db):
    token = WbToken(
        user_id=1,
        encrypted_token="stub",
        token_status="active",
        is_active=True,
        sync_in_progress=True,
    )
    db.add(token)
    db.commit()
    db.refresh(token)

    job = create_sync_job(db, user_id=1, wb_token_id=token.id, sync_type="manual_sync")
    step_map = {step.step_name: step for step in job.steps}
    finished_at = datetime(2026, 5, 5, 12, 0, 0)
    step_map["token_check"].status = "completed"
    step_map["products"].status = "completed"
    step_map["stocks"].status = "skipped"
    step_map["stocks"].error_message = "rate limited"
    for step in step_map.values():
        if step.status in {"completed", "skipped"}:
            step.finished_at = finished_at
    job.status = "queued"
    job.finished_at = finished_at
    job.progress_percent = 100
    db.commit()

    status = get_latest_sync_status(db, 1)
    db.refresh(job)
    db.refresh(token)

    assert status["status"] == "partial"
    assert job.status == "partial"
    assert token.sync_in_progress is False
