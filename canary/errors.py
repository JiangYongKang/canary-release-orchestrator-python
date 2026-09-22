"""可区分的业务异常链路。

每类拒绝/失败都有独立的 ``code``，便于 API 层映射不同 HTTP 状态与测试断言。
"""

from __future__ import annotations


class OrchestratorError(Exception):
    """所有编排业务异常的基类。"""

    code: str = "orchestrator_error"
    http_status: int = 400

    def __init__(
        self,
        message: str,
        *,
        release_id: str | None = None,
        remaining_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.release_id = release_id
        self.remaining_seconds = remaining_seconds

    def to_dict(self) -> dict:
        return {
            "error": self.code,
            "message": self.message,
            "release_id": self.release_id,
            "remaining_seconds": self.remaining_seconds,
        }


class DuplicateSubmissionError(OrchestratorError):
    """重复提交：相同 release_id 的计划已存在。"""

    code = "duplicate_submission"
    http_status = 409


class IllegalTransitionError(OrchestratorError):
    """非法状态转换：当前状态不允许执行该动作。"""

    code = "illegal_transition"
    http_status = 409


class ObservationPendingError(OrchestratorError):
    """当前批次尚在最小观察期内，推进被拒绝。"""

    code = "observation_pending"
    http_status = 425  # 425 Too Early：语义上表示“观察期未满，过早推进”


class ProbeTimeoutError(OrchestratorError):
    """健康探测超时异常链路。"""

    code = "probe_timeout"
    http_status = 504


class ConsecutiveFailureError(OrchestratorError):
    """连续健康失败超过阈值的异常链路。"""

    code = "consecutive_failures"
    http_status = 502


class ManualAbortError(OrchestratorError):
    """人工中止异常链路。"""

    code = "manual_abort"
    http_status = 409


class IncompatibleStateError(OrchestratorError):
    """磁盘上的状态数据与当前代码不兼容。"""

    code = "incompatible_state"
    http_status = 500
