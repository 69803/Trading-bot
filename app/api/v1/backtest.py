"""Backtest run endpoints: create, list, retrieve, status, results."""

import uuid
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.models.backtest_run import BacktestRun
from app.models.user import User
from app.schemas.backtest import BacktestListResponse, BacktestRunOut, BacktestRunRequest
from app.services import backtest_service

router = APIRouter()


@router.get("", response_model=BacktestListResponse, summary="List backtests")
@router.get("/history", response_model=BacktestListResponse, summary="Backtest history (alias)")
async def list_backtests(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BacktestListResponse:
    runs, total = await backtest_service.get_backtest_runs(
        db,
        user_id=current_user.id,
        limit=page_size,
        offset=(page - 1) * page_size,
    )
    return BacktestListResponse(items=runs, total=total)


@router.post(
    "/run",
    response_model=BacktestRunOut,
    status_code=status.HTTP_201_CREATED,
    summary="Queue a new backtest run and launch background task",
)
@router.post(
    "",
    response_model=BacktestRunOut,
    status_code=status.HTTP_201_CREATED,
    summary="Queue a new backtest run (root POST)",
    include_in_schema=False,
)
async def create_backtest(
    body: BacktestRunRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BacktestRun:
    """Create a BacktestRun record and immediately launch an asyncio background task."""
    params = {
        "symbol": body.symbol.upper(),
        "timeframe": body.timeframe,
        "start_date": body.start_date.isoformat() if hasattr(body.start_date, "isoformat") else body.start_date,
        "end_date": body.end_date.isoformat() if hasattr(body.end_date, "isoformat") else body.end_date,
        "initial_capital": float(body.initial_capital),
        "ema_fast": body.ema_fast,
        "ema_slow": body.ema_slow,
        "rsi_period": body.rsi_period,
        "rsi_overbought": body.rsi_overbought,
        "rsi_oversold": body.rsi_oversold,
        "stop_loss_pct": body.stop_loss_pct,
        "take_profit_pct": body.take_profit_pct,
        "commission": body.commission_pct,
        "use_sentiment": body.use_sentiment,
        "position_size_pct": body.position_size_pct,
    }
    run = await backtest_service.create_backtest_run(
        db, user_id=current_user.id, params=params
    )
    return run


@router.get("/{run_id}/status", response_model=BacktestRunOut, summary="Get backtest status")
async def get_backtest_status(
    run_id: uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BacktestRun:
    try:
        return await backtest_service.get_backtest_run(db, run_id=run_id, user_id=current_user.id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.get("/{run_id}/results", summary="Get backtest results")
async def get_backtest_results(
    run_id: uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        run = await backtest_service.get_backtest_run(db, run_id=run_id, user_id=current_user.id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))

    if run.status not in ("completed", "failed"):
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail=f"Backtest is still {run.status} ({run.progress_pct}%)",
        )
    return {
        "id": str(run.id),
        "status": run.status,
        "progress_pct": run.progress_pct,
        "results": run.results,
        "equity_curve": run.equity_curve,
        "trade_log": run.trade_log,
        "error_message": run.error_message,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
    }


@router.get("/{run_id}", response_model=BacktestRunOut, summary="Get backtest details")
async def get_backtest(
    run_id: uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BacktestRun:
    try:
        return await backtest_service.get_backtest_run(db, run_id=run_id, user_id=current_user.id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))


@router.delete(
    "/{run_id}",
    status_code=status.HTTP_200_OK,
    summary="Delete a backtest run",
)
async def delete_backtest(
    run_id: uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    result = await db.execute(
        select(BacktestRun).where(
            BacktestRun.id == run_id, BacktestRun.user_id == current_user.id
        )
    )
    run: BacktestRun | None = result.scalar_one_or_none()
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Backtest run not found"
        )
    if run.status == "running":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot delete a running backtest",
        )
    await db.delete(run)
    await db.commit()
    return {"deleted": True, "run_id": str(run_id)}
