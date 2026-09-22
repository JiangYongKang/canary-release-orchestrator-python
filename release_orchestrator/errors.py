"""可区分的业务异常类型（骨架：仅类型定义）。

每种拒绝/失败链路对应独立异常类，API 层映射为不同错误码，
保证"非法状态转换"、"重复提交"、"探测超时"、"连续失败"、"人工中止"
在调用方均可被程序化区分。
"""

from __future__ import annotations


class OrchestratorError(Exception):
    """所有编排器异常的基类。"""

    error_code: str = "orchestrator_error"


class ReleaseNotFoundError(OrchestratorError):
    error_code = "release_not_found"


class DuplicateReleaseError(OrchestratorError):
    """重复提交：同名称发布已存在。"""

    error_code = "duplicate_release"


class IllegalTransitionError(OrchestratorError):
    """非法状态转换：当前状态不允许执行目标操作。"""

    error_code = "illegal_transition"


class DuplicateAdvanceError(OrchestratorError):
    """重复推进：批次尚未满足再次推进条件（观察期未满 / 已暂停等待恢复）。"""

    error_code = "duplicate_advance"


class AdvanceConflictError(OrchestratorError):
    """并发冲突：同一发布已有推进在执行中，本次调用未生效。"""

    error_code = "advance_conflict"


class ProbeTimeoutError(OrchestratorError):
    """健康探测超时链路。"""

    error_code = "probe_timeout"


class ConsecutiveFailureError(OrchestratorError):
    """连续健康失败达到阈值链路。"""

    error_code = "consecutive_failure"


class ManualAbortError(OrchestratorError):
    """人工中止链路。"""

    error_code = "manual_abort"


class IncompatibleStateError(OrchestratorError):
    """历史状态数据与当前版本不兼容。"""

    error_code = "incompatible_state"
