from models.entities import WbToken
from services.background_sync import claim_next_sync_job, create_sync_job


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
