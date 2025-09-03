import aiohttp
import asyncio
import json
import logging
import os
import re
from collections.abc import Sequence
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple, Union

# ---------------------------
# ЛОГУВАННЯ У ФАЙЛ (DEBUG)
# ---------------------------
_LOGGER = logging.getLogger("teltonika_fota")
if not _LOGGER.handlers:
    _LOGGER.setLevel(logging.DEBUG)
    _fh = logging.FileHandler("fota_api_debug.log", encoding="utf-8")
    _fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    _fh.setFormatter(_fmt)
    _LOGGER.addHandler(_fh)

def _mask_token(tok: Optional[str]) -> str:
    if not tok:
        return "<empty>"
    t = str(tok)
    if len(t) <= 6:
        return "***"
    return t[:3] + "..." + t[-3:]

def _truncate(val: Any, limit: int = 1000) -> str:
    try:
        s = val if isinstance(val, str) else json.dumps(val, ensure_ascii=False)
    except Exception:
        s = str(val)
    if len(s) > limit:
        return s[:limit] + "…"
    return s


def _parse_retry_after(header: Optional[str]) -> Optional[float]:
    """Parse a ``Retry-After`` header into seconds."""
    if not header:
        return None
    header = header.strip()
    try:
        return float(header)
    except ValueError:
        try:
            dt = parsedate_to_datetime(header)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            return None

def _build_params(
    *,
    page: Optional[int] = None,
    per_page: Optional[int] = None,
    sort: Optional[Union[str, Sequence[str]]] = None,
    order: Optional[Union[str, Sequence[str]]] = None,
    **filters: Any,
) -> List[Tuple[str, Any]]:
    """Construct a list of query parameters from common arguments and filters.

    Sequences (other than strings/bytes) are expanded into repeated
    parameters as required by the FOTA Web API.
    """

    params: List[Tuple[str, Any]] = []
    if page is not None:
        params.append(("page", page))
    if per_page is not None:
        params.append(("per_page", per_page))

    def _extend(key: str, value: Any) -> None:
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            params.extend((key, item) for item in value)
        else:
            params.append((key, value))

    if sort is not None:
        _extend("sort", sort)
    if order is not None:
        _extend("order", order)

    for k, v in filters.items():
        if v is None:
            continue
        _extend(k, v)

    return params


def _unwrap_data(data: Any, aggressive: bool = False) -> Any:
    """Unwrap common ``{"data": ...}`` or ``{"data": {"items": [...]}}`` responses.

    Args:
        data: Parsed JSON payload.
        aggressive: When ``True``, unwrap even if sibling metadata is present,
            effectively dropping it.

    Returns:
        The innermost value for known patterns, otherwise the original input.
    """

    if not isinstance(data, dict):
        return data

    if "data" in data and (len(data) == 1 or aggressive):
        data = data["data"]

    if isinstance(data, dict) and "items" in data and (len(data) == 1 or aggressive):
        return data["items"]

    return data

# ---------------------------
# ПОМИЛКИ
# ---------------------------
class FotaWebApiError(Exception):
    """Исключение для ошибок запросов к FOTA Web API."""
    def __init__(self, status: int, message: str, details: Optional[Any] = None):
        super().__init__(f"HTTP {status} Error: {message}")
        self.status = status
        self.details = details


class FotaRateLimitError(FotaWebApiError):
    """Ошибка превышения лимита запросов после повторных попыток."""

    def __init__(
        self,
        status: int,
        message: str,
        *,
        retry_after: Optional[float] = None,
        attempts: int = 0,
        details: Optional[Any] = None,
    ):
        super().__init__(status, message, details=details)
        self.retry_after = retry_after
        self.attempts = attempts

# ---------------------------
# КЛІЄНТ
# ---------------------------
class FotaClient:
    """Клиент для Teltonika FOTA Web API, асинхронный."""
    def __init__(
        self,
        token: str,
        base_url: str = "https://api.teltonika.lt",
        user_agent: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        max_attempts: int = 3,
        backoff_factor: float = 1.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._max_attempts = max(1, int(max_attempts))
        self._backoff_factor = float(backoff_factor)


        if session is None:
            if user_agent is None:
                user_agent = "TeltonikaFotaClient/1.2"
                _LOGGER.warning(
                    "Using default user_agent '%s'. Provide a custom user_agent in the format 'CompanyName AppName/Version'.",
                    user_agent,
                )
            else:
                if not re.match(r"^[^\s]+\s[^\s]+/[^\s]+$", user_agent):
                    raise ValueError(
                        "user_agent must be in the format 'CompanyName AppName/Version'"
                    )

            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": user_agent,
            }
            self.session = aiohttp.ClientSession(headers=headers, trust_env=True)
            self._owns_session = True
        else:
            if user_agent is not None and not re.match(r"^[^\s]+\s[^\s]+/[^\s]+$", user_agent):
                raise ValueError(
                    "user_agent must be in the format 'CompanyName AppName/Version'"
                )
            self.session = session
            self._owns_session = False

            headers_obj = getattr(self.session, "headers", None)
            current_ua = (
                headers_obj.get("User-Agent")
                if headers_obj is not None and hasattr(headers_obj, "get")
                else None
            )
            if not current_ua or not re.match(r"^[^\s]+\s[^\s]+/[^\s]+$", current_ua):
                if user_agent is not None:
                    new_ua = user_agent
                elif not current_ua:
                    new_ua = "TeltonikaFotaClient/1.2"
                    _LOGGER.warning(
                        "Using default user_agent '%s'. Provide a custom user_agent in the format 'CompanyName AppName/Version'.",
                        new_ua,
                    )
                else:
                    raise ValueError(
                        "session User-Agent must be in the format 'CompanyName AppName/Version'"
                    )

                if hasattr(self.session, "_default_headers"):
                    self.session._default_headers["User-Agent"] = new_ua
                else:
                    if headers_obj is None or not hasattr(headers_obj, "__setitem__"):
                        headers_obj = {}
                        setattr(self.session, "headers", headers_obj)
                    headers_obj["User-Agent"] = new_ua

        # Розділи API
        self.devices = DeviceAPI(self)
        self.tasks = TaskAPI(self)
        self.batches = BatchAPI(self)
        self.groups = GroupAPI(self)
        self.files = FileAPI(self)
        self.firmwares = FirmwareAPI(self)
        self.configurations = ConfigurationAPI(self)
        self.companies = CompanyAPI(self)
        self.users = UserAPI(self)
        self.background_actions = BackgroundActionAPI(self)
        self.can_adapters = CanAdapterAPI(self)

    async def _request(self, method: str, path: str, *, unwrap: bool = True, **kwargs) -> Any:
        """Низкоуровневый вызов HTTP."""
        url = self.base_url + path
        params = kwargs.get("params")
        body_json = kwargs.get("json")
        body_data = kwargs.get("data")

        # Лог запиту
        _LOGGER.debug(
            "REQUEST %s %s | headers.Authorization=%s | params=%s | json=%s | data=%s",
            method, url, _mask_token(self.token),
            _truncate(params), _truncate(body_json), "<form-data>" if body_data else None
        )

        for attempt in range(1, self._max_attempts + 1):
            async with self.session.request(method, url, **kwargs) as resp:
                # Бінарний?
                is_binary = (kwargs.get("params", {}) or {}).get("download") or kwargs.get("stream")
                txt = None
                data_json = None

                if resp.status == 429:
                    try:
                        data_json = await resp.json()
                        txt = data_json
                    except Exception:
                        try:
                            txt = await resp.text()
                        except Exception:
                            txt = None
                    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                    _LOGGER.warning(
                        "RESPONSE %s %s | status=429 | body=%s | attempt=%s/%s",
                        method, url, _truncate(txt), attempt, self._max_attempts
                    )
                    if attempt >= self._max_attempts:
                        raise FotaRateLimitError(
                            resp.status,
                            _truncate(txt),
                            retry_after=retry_after,
                            attempts=attempt,
                            details=txt,
                        )
                    delay = retry_after if retry_after is not None else self._backoff_factor * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    continue

                if resp.status >= 400:
                    # читаємо помилку
                    try:
                        data_json = await resp.json()
                        txt = data_json
                    except Exception:
                        try:
                            txt = await resp.text()
                        except Exception:
                            txt = None
                    _LOGGER.error(
                        "RESPONSE %s %s | status=%s | body=%s",
                        method, url, resp.status, _truncate(txt)
                    )
                    raise FotaWebApiError(resp.status, _truncate(txt), details=txt)

                if is_binary:
                    content = await resp.read()
                    _LOGGER.debug(
                        "RESPONSE %s %s | status=%s | bytes=%s",
                        method, url, resp.status, len(content)
                    )
                    return content

                try:
                    data_json = await resp.json()
                    _LOGGER.debug(
                        "RESPONSE %s %s | status=%s | json=%s",
                        method, url, resp.status, _truncate(data_json)
                    )
                    if unwrap:
                        return _unwrap_data(data_json, aggressive=True)
                    return data_json
                except aiohttp.ContentTypeError:
                    txt = await resp.text()
                    _LOGGER.debug(
                        "RESPONSE %s %s | status=%s | text=%s",
                        method, url, resp.status, _truncate(txt)
                    )
                    return None

    async def close(self):
        if self._owns_session and not self.session.closed:
            await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

# ---------------------------
# DEVICES
# ---------------------------
class DeviceAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        page: int = 1,
        per_page: int = 25,
        sort: Optional[Union[str, Sequence[str]]] = None,
        order: Optional[Union[str, Sequence[str]]] = None,
        with_meta: bool = False,
        **filters
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List devices with optional filters and pagination.

        Parameters
        ----------
        page: int, optional
            Page number starting from 1.
        per_page: int, optional
            Number of items per page.
        sort: Union[str, Sequence[str]], optional
            Field(s) to sort by. A sequence results in multiple ``sort``
            query parameters.
        order: Union[str, Sequence[str]], optional
            Sort direction(s) (``asc`` or ``desc``).
        with_meta: bool, optional
            If ``True``, return the raw response including pagination
            metadata such as ``total`` or ``pages``. Otherwise only the
            list of devices is returned.
        **filters: Any
            Additional filter parameters accepted by the API. Sequence
            values are serialized as repeated query parameters.

        Returns
        -------
        Union[List[Dict[str, Any]], Dict[str, Any]]
            Either a list of devices or the raw response dictionary
            depending on ``with_meta``. The backend may wrap results in
            ``{"data": {...}}`` or ``{"data": {"items": [...]}}``;
            such wrappers are removed unless ``with_meta`` is ``True``.
        """
        params = _build_params(
            page=page, per_page=per_page, sort=sort, order=order, **filters
        )
        kwargs: Dict[str, Any] = {"params": params or None}
        if with_meta:
            kwargs["unwrap"] = False
        data = await self.client._request("GET", "/devices", **kwargs)
        data = _unwrap_data(data, aggressive=not with_meta)
        if with_meta:
            return data or {}
        return data or []

    async def get(self, device_imei: Union[str, int]) -> Dict[str, Any]:
        """Retrieve a single device by IMEI.

        Supports backend responses like ``{"imei": ...}`` or
        ``{"data": {"imei": ...}}``. Unknown payloads are returned
        unchanged.
        """
        imei_str = str(device_imei).strip()
        data = await self.client._request("GET", f"/devices/{imei_str}")
        return _unwrap_data(data, aggressive=True)

    async def get_by_imei(self, device_imei: Union[str, int]) -> Optional[Dict[str, Any]]:
        try:
            return await self.get(device_imei)
        except FotaWebApiError as e:
            if e.status == 404:
                return None
            raise

    async def update(self, device_imei: Union[str, int], **fields) -> Dict[str, Any]:
        """Partially update device fields via ``PATCH``.

        Use :meth:`replace` for full device replacement using ``PUT``.
        """
        imei_str = str(device_imei).strip()
        if not fields:
            return await self.get(imei_str)
        return await self.client._request("PATCH", f"/devices/{imei_str}", json=fields)

    async def replace(self, device_imei: Union[str, int], **fields) -> Dict[str, Any]:
        """Replace the device resource using ``PUT``.

        This method expects the complete device representation.
        """
        imei_str = str(device_imei).strip()
        if not fields:
            return await self.get(imei_str)
        return await self.client._request("PUT", f"/devices/{imei_str}", json=fields)

    async def export(
        self,
        source: str,
        format: str,
        id_list: Optional[List[Union[str, int]]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
        columns: Optional[List[str]] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Export devices using an ID list or filter.

        Args:
            source: Selection source, e.g. ``"id_list"`` or ``"filter"``.
            format: Desired export format (``"csv"``, ``"xlsx"``, etc.).
            id_list: List of device IMEIs when ``source="id_list"``.
            filter: List of filter objects when ``source="filter"``.
            columns: Optional list of column names to include.
            description: Optional export description.
        """
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")

        payload: Dict[str, Any] = {"source": source, "format": format}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter
        if columns is not None:
            payload["columns"] = columns
        if description is not None:
            payload["description"] = description

        return await self.client._request("POST", "/devices/export", json=payload)

    async def stats(self, field: str, **filters) -> Dict[str, Any]:
        """Return device statistics grouped by ``field``.

        ``filters`` values may be scalars or lists. Lists are serialized as
        repeated query parameters per API contract.
        """

        params: List[tuple] = [("field", field)]
        for k, v in filters.items():
            if v is None:
                continue
            if isinstance(v, list):
                params.extend((k, item) for item in v)
            else:
                params.append((k, v))

        return await self.client._request("GET", "/devices/stats", params=params)

    async def filter_list(
        self,
        field: str,
        company_id: Optional[Union[int, List[int]]] = None,
    ) -> Dict[str, Any]:
        """Retrieve available values for a filter field.

        Args:
            field: Name of the field for which to return distinct values.
            company_id: Optional company identifier(s) to scope the search.

        Returns:
            Parsed JSON response.
        """

        params: List[tuple] = [("field", field)]
        if company_id is not None:
            if isinstance(company_id, list):
                params.extend(("company_id", cid) for cid in company_id)
            else:
                params.append(("company_id", company_id))

        return await self.client._request("GET", "/devices/filterList", params=params)

    async def bulk_update(
        self,
        source: str,
        data: Dict[str, Any],
        id_list: Optional[List[Union[str, int]]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
        id_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Bulk update devices using either an ID list or a filter.

        Args:
            source: Selection source, ``"id_list"`` or ``"filter"``.
            data: Fields to update for matching devices.
            id_list: List of device identifiers when ``source="id_list"``.
            filter: List of filter objects when ``source="filter"``.
            id_type: Optional type of identifiers provided in ``id_list`` or
                ``filter``. Accepts ``"id"``, ``"imei"`` or ``"sn"``.
        """

        if source not in {"id_list", "filter"}:
            raise ValueError("source must be 'id_list' or 'filter'")
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")

        payload: Dict[str, Any] = {"source": source, "data": data}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter
        if id_type is not None:
            payload["id_type"] = id_type

        return await self.client._request("POST", "/devices/bulkUpdate", json=payload)

    async def changes(
        self,
        device_imei: Union[str, int],
        sort: Optional[Union[str, List[str]]] = None,
        page: int = 1,
        per_page: int = 25,
        order: Optional[Union[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """Retrieve change history for a device."""

        imei_str = str(device_imei).strip()
        params: List[tuple] = [("page", page), ("per_page", per_page)]
        if sort:
            if isinstance(sort, list):
                params.extend(("sort", s) for s in sort)
            else:
                params.append(("sort", sort))
        if order:
            if isinstance(order, list):
                params.extend(("order", o) for o in order)
            else:
                params.append(("order", order))

        return await self.client._request(
            "GET", f"/devices/{imei_str}/changes", params=params
        )
        
    # The FOTA Web API exposes internal endpoints for generating transfer tokens
    # and transferring device ownership. These endpoints are not part of the
    # public specification and related helper methods have been removed from the
    # client. Use with caution if interacting with them directly.

# ---------------------------
# TASKS
# ---------------------------
class TaskAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        page: int = 1,
        per_page: int = 25,
        sort: Optional[Union[str, Sequence[str]]] = None,
        order: Optional[Union[str, Sequence[str]]] = None,
        with_meta: bool = False,
        **filters
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List tasks with optional filters and pagination.

        Set ``with_meta=True`` to receive the raw response including
        pagination metadata. Sequence values in ``sort``, ``order`` or
        ``filters`` are expanded into repeated query parameters.
        """
        params = _build_params(
            page=page, per_page=per_page, sort=sort, order=order, **filters
        )
        kwargs: Dict[str, Any] = {"params": params}
        if with_meta:
            kwargs["unwrap"] = False
        data = await self.client._request("GET", "/tasks", **kwargs)
        data = _unwrap_data(data, aggressive=not with_meta)
        if with_meta:
            return data or {}
        return data or []

    async def filter_list(self, field: str, **filters) -> Dict[str, Any]:
        """Retrieve available values for a task filter field."""
        params: List[Tuple[str, Any]] = [("field", field)]
        for k, v in filters.items():
            if v is None:
                continue
            if isinstance(v, Sequence) and not isinstance(v, (str, bytes, bytearray)):
                for item in v:
                    params.append((k, item))
            else:
                params.append((k, v))
        return await self.client._request("GET", "/tasks/filterList", params=params)

    async def get(self, task_id: int) -> Dict[str, Any]:
        data = await self.client._request("GET", f"/tasks/{task_id}")
        return _unwrap_data(data, aggressive=True)

    async def create(
        self,
        device_imei: Union[str, int],
        file_id: Optional[int] = None,
        task_type: Optional[str] = None,
        schedule: Optional[Dict[str, Any]] = None,
        expire_existing_tasks: Optional[bool] = None,
        **params
    ) -> Dict[str, Any]:
        """Create a task for a device.

        The task type must be provided either via ``task_type`` or by supplying
        a ``type`` key within ``params``.

        Args:
            device_imei: Target device IMEI.
            file_id: Optional file identifier.
            task_type: Task type string. Overrides ``params['type']`` if both
                are supplied.
            schedule: Optional schedule object matching swagger schema with
                ``type`` and ``attributes`` keys.
            expire_existing_tasks: Whether to expire tasks of the same type.
            **params: Extra fields to include in the payload. Used to pass
                ``type`` when ``task_type`` is not given.

        Returns:
            Parsed JSON response from the API.

        Raises:
            ValueError: If ``device_imei`` or task ``type`` is missing, or
                ``schedule`` is malformed.
        """
        imei_str = str(device_imei).strip()
        if not imei_str:
            raise ValueError("device_imei is required")
        imei_int = int(imei_str)
        payload: Dict[str, Any] = {"device_imei": imei_int}
        if file_id is not None:
            payload["file_id"] = file_id
        if task_type is not None:
            payload["type"] = task_type
            params.pop("type", None)
        elif "type" in params:
            pass
        else:
            raise ValueError("task_type (or 'type') is required")
        if schedule is not None:
            if not isinstance(schedule, dict) or "type" not in schedule:
                raise ValueError("schedule must be a dict with a 'type' key")
            sched_payload = {"type": schedule["type"]}
            if "attributes" in schedule:
                if not isinstance(schedule["attributes"], dict):
                    raise ValueError("schedule['attributes'] must be a dict")
                sched_payload["attributes"] = schedule["attributes"]
            payload["schedule"] = sched_payload
        if isinstance(expire_existing_tasks, bool):
            payload["expire_existing_tasks"] = expire_existing_tasks
        payload.update(params)
        return await self.client._request("POST", "/tasks", json=payload)

    async def create_can_oem_upload_by_imei(
        self,
        imei: Union[str, int],
        vehicle_id: int,
        expire_existing_tasks: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Приклад із Swagger для CAN OEM:
        {
          "attributes": "{\"vehicle_id\":10013}",
          "device_imei": 350000000000001,
          "type": "TxCanConfiguration"
        }
        """
        imei_int = int(str(imei).strip())
        attributes_str = json.dumps({"vehicle_id": int(vehicle_id)}, separators=(",", ":"))
        payload: Dict[str, Any] = {
            "device_imei": imei_int,
            "type": "TxCanConfiguration",
            "attributes": attributes_str
        }
        if isinstance(expire_existing_tasks, bool):
            payload["expire_existing_tasks"] = expire_existing_tasks
        return await self.client._request("POST", "/tasks", json=payload)

    async def bulk_create(
        self,
        source: str,
        data: Dict[str, Any],
        id_list: Optional[List[int]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Create multiple tasks using either an ID list or a filter."""
        if source not in {"id_list", "filter"}:
            raise ValueError("source must be 'id_list' or 'filter'")
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")
        if "device_imei" in data:
            data["device_imei"] = int(str(data["device_imei"]).strip())
        payload: Dict[str, Any] = {"source": source, "data": data}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter
        return await self.client._request("POST", "/tasks/bulkCreate", json=payload)

    async def cancel(self, task_id: int) -> Dict[str, Any]:
        """Cancel a single task.

        Returns a dictionary containing the ``background_action_id`` that can
        be used to track cancellation progress.

        Example::

            resp = await client.tasks.cancel(12345)
            await client.background_actions.get(resp["background_action_id"])

        """
        return await self.client._request("POST", f"/tasks/{task_id}/cancel", json={})

    async def bulk_cancel(
        self,
        source: str,
        id_list: Optional[List[int]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Cancel multiple tasks using one of two sources.

        Returns a dictionary containing the ``background_action_id`` for
        tracking asynchronous cancellation.

        Args:
            source: Source of tasks to cancel. Must be ``"id_list"`` or ``"filter"``.
            id_list: List of task IDs when ``source="id_list"``.
            filter: List of filter objects when ``source="filter"``.

        Examples:

            Cancel by task IDs::

                resp = await client.tasks.bulk_cancel(source="id_list", id_list=[1, 2, 3])

            Cancel by filter::

                resp = await client.tasks.bulk_cancel(
                    source="filter", filter=[{"field": "status", "value": "pending"}]
                )

        """
        if source not in {"id_list", "filter"}:
            raise ValueError("source must be 'id_list' or 'filter'")
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")
        payload: Dict[str, Any] = {"source": source}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter
        return await self.client._request("POST", "/tasks/bulkCancel", json=payload)

# ---------------------------
# BATCHES
# ---------------------------
class BatchAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def get(self, batch_id: int) -> Dict[str, Any]:
        data = await self.client._request("GET", f"/batches/{batch_id}")
        return _unwrap_data(data, aggressive=True)

    async def list(
        self,
        page: int = 1,
        per_page: int = 25,
        sort: Optional[Union[str, Sequence[str]]] = None,
        order: Optional[Union[str, Sequence[str]]] = None,
        with_meta: bool = False,
        **filters
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List batches with optional filters and pagination.

        Set ``with_meta=True`` to receive the raw response including
        pagination metadata. Sequence values in ``sort``, ``order`` or
        ``filters`` are expanded into repeated query parameters.
        """
        params = _build_params(
            page=page, per_page=per_page, sort=sort, order=order, **filters
        )
        kwargs: Dict[str, Any] = {"params": params}
        if with_meta:
            kwargs["unwrap"] = False
        data = await self.client._request("GET", "/batches", **kwargs)
        data = _unwrap_data(data, aggressive=not with_meta)
        if with_meta:
            return data or {}
        return data or []

    async def filter_list(
        self,
        field: str,
        company_id: Optional[Union[int, List[int]]] = None,
    ) -> Dict[str, Any]:
        params: List[tuple] = [("field", field)]
        if company_id is not None:
            if isinstance(company_id, list):
                params.extend(("company_id", cid) for cid in company_id)
            else:
                params.append(("company_id", company_id))
        return await self.client._request(
            "GET", "/batches/filterList", params=params
        )

    async def retry_failed_tasks(self, batch_id: int) -> Dict[str, Any]:
        return await self.client._request(
            "POST", f"/batches/{batch_id}/retryFailedTasks", json={}
        )

# ---------------------------
# GROUPS
# ---------------------------
class GroupAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        page: int = 1,
        per_page: int = 25,
        sort: Optional[Union[str, Sequence[str]]] = None,
        order: Optional[Union[str, Sequence[str]]] = None,
        with_meta: bool = False,
        **filters,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List groups with optional filters and pagination.

        Additional filters can be supplied as keyword arguments. Sequence
        values are serialized as repeated query parameters.
        """
        params = _build_params(
            page=page, per_page=per_page, sort=sort, order=order, **filters
        )

        kwargs: Dict[str, Any] = {"params": params or None}
        if with_meta:
            kwargs["unwrap"] = False
        data = await self.client._request("GET", "/groups", **kwargs)
        data = _unwrap_data(data, aggressive=not with_meta)
        if with_meta:
            return data or {}
        return data or []

    async def filter_list(
        self,
        field: str,
        company_id: Optional[Union[int, List[int]]] = None,
    ) -> Dict[str, Any]:
        """Retrieve available values for a filter field."""

        params: List[tuple] = [("field", field)]
        if company_id is not None:
            if isinstance(company_id, list):
                params.extend(("company_id", cid) for cid in company_id)
            else:
                params.append(("company_id", company_id))

        return await self.client._request("GET", "/groups/filterList", params=params)

    async def get(self, group_id: int) -> Dict[str, Any]:
        data = await self.client._request("GET", f"/groups/{group_id}")
        return _unwrap_data(data, aggressive=True)

    async def create(
        self,
        name: str,
        company_id: int,
        firmware_file_id: Optional[int] = None,
        config_file_id: Optional[int] = None
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"name": name, "company_id": company_id}
        if firmware_file_id is not None:
            payload["firmware_file_id"] = firmware_file_id
        if config_file_id is not None:
            payload["configuration_file_id"] = config_file_id
        return await self.client._request("POST", "/groups", json=payload)

    async def update(self, group_id: int, **fields) -> Dict[str, Any]:
        """Partially update group fields via ``PATCH``.

        Use :meth:`replace` for full group replacement using ``PUT``.
        """
        if not fields:
            return await self.get(group_id)
        return await self.client._request("PATCH", f"/groups/{group_id}", json=fields)

    async def replace(self, group_id: int, **fields) -> Dict[str, Any]:
        """Replace the group resource using ``PUT``."""
        if not fields:
            return await self.get(group_id)
        return await self.client._request("PUT", f"/groups/{group_id}", json=fields)

    async def _delete_unofficial(self, group_id: int) -> None:
        """Attempt to delete a single group via an undocumented endpoint.

        The public FOTA Web API does not expose a dedicated endpoint for
        deleting individual groups.  Use :meth:`bulk_delete` instead for
        officially supported deletion.  This helper exists only for
        completeness and may cease to work without notice.
        """

        await self.client._request("DELETE", f"/groups/{group_id}")

    async def bulk_delete(
        self,
        source: str,
        id_list: Optional[List[int]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Bulk delete groups using either an ID list or a filter."""

        if source not in {"id_list", "filter"}:
            raise ValueError("source must be 'id_list' or 'filter'")
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")

        payload: Dict[str, Any] = {"source": source}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter

        return await self.client._request("POST", "/groups/bulkDelete", json=payload)

# ---------------------------
# FILES
# ---------------------------
class FileAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        page: int = 1,
        per_page: int = 25,
        sort: Optional[Union[str, Sequence[str]]] = None,
        order: Optional[Union[str, Sequence[str]]] = None,
        with_meta: bool = False,
        **filters,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List files available in the FOTA service.

        Filters should be supplied as keyword arguments. Sequence values are
        serialized as repeated query parameters. For example::

            await api.files.list(type=["firmware", "configuration"], company_id=[1, 2])

        Set ``with_meta=True`` to receive the full JSON response including
        pagination metadata. By default only the list of files is returned.
        """
        params = _build_params(
            page=page, per_page=per_page, sort=sort, order=order, **filters
        )

        kwargs: Dict[str, Any] = {"params": params or None}
        if with_meta:
            kwargs["unwrap"] = False
        data = await self.client._request("GET", "/files", **kwargs)
        data = _unwrap_data(data, aggressive=not with_meta)
        if with_meta:
            return data or {}
        return data or []

    async def filter_list(
        self,
        field: str,
        company_id: Optional[Union[int, List[int]]] = None,
    ) -> Dict[str, Any]:
        """Retrieve available values for a filter field."""

        params: List[tuple] = [("field", field)]
        if company_id is not None:
            if isinstance(company_id, list):
                params.extend(("company_id", cid) for cid in company_id)
            else:
                params.append(("company_id", company_id))

        return await self.client._request("GET", "/files/filterList", params=params)

    async def upload(
        self,
        file_path: str,
        file_type: str,
        description: str = "",
        company_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Upload a file to the FOTA service.

        Parameters
        ----------
        file_path: str
            Path to the file on disk.
        file_type: str
            File category. Must be one of ``firmware``, ``configuration``,
            ``certificate``, ``ble_fw`` or ``blue_nrg``. The value is case-insensitive
            but will always be sent to the API in lower-case.
        description: str
            Optional human readable description.
        company_ids: Optional[List[int]]
            Optional list of company IDs that should see the file.
        """
        form = aiohttp.FormData()
        f = open(file_path, "rb")
        try:
            form.add_field(
                "file",
                f,
                filename=os.path.basename(file_path),
                content_type="application/octet-stream",
            )
            form.add_field("type", file_type.lower())
            if description:
                form.add_field("description", description)
            if company_ids is not None:
                for company_id in company_ids:
                    form.add_field("company_ids", str(company_id))
            return await self.client._request("POST", "/files", data=form)
        finally:
            f.close()

    async def upload_certificate(
        self,
        file_path: str,
        description: str = "",
        company_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Convenience wrapper for uploading certificate files."""
        return await self.upload(
            file_path,
            file_type="certificate",
            description=description,
            company_ids=company_ids,
        )

    async def upload_ble_firmware(
        self,
        file_path: str,
        description: str = "",
        company_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Convenience wrapper for uploading BLE firmware files."""
        return await self.upload(
            file_path,
            file_type="ble_fw",
            description=description,
            company_ids=company_ids,
        )

    async def upload_blue_nrg_firmware(
        self,
        file_path: str,
        description: str = "",
        company_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Convenience wrapper for uploading BlueNRG firmware files."""
        return await self.upload(
            file_path,
            file_type="blue_nrg",
            description=description,
            company_ids=company_ids,
        )

    async def download(self, file_id: int) -> bytes:
        return await self.client._request(
            "GET", f"/files/download/{file_id}", stream=True
        )

    async def get(self, file_id: int) -> Dict[str, Any]:
        data = await self.client._request("GET", f"/files/{file_id}")
        return _unwrap_data(data, aggressive=True)

    async def update(
        self,
        file_id: int,
        description: Optional[str] = None,
        company_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Partially update file metadata via ``PATCH``.

        Use :meth:`replace` for full file replacement using ``PUT``.
        """
        payload: Dict[str, Any] = {}
        if description is not None:
            payload["description"] = description
        if company_id is not None:
            payload["company_id"] = company_id
        return await self.client._request("PATCH", f"/files/{file_id}", json=payload)

    async def replace(
        self,
        file_id: int,
        description: Optional[str] = None,
        company_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Replace file metadata using ``PUT``."""
        payload: Dict[str, Any] = {}
        if description is not None:
            payload["description"] = description
        if company_id is not None:
            payload["company_id"] = company_id
        return await self.client._request("PUT", f"/files/{file_id}", json=payload)

    async def delete(self, file_id: int) -> None:
        await self.client._request("DELETE", f"/files/{file_id}")

    async def bulk_delete(
        self,
        source: str,
        id_list: Optional[List[int]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Bulk delete files using either an ID list or a filter."""

        if source not in {"id_list", "filter"}:
            raise ValueError("source must be 'id_list' or 'filter'")
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")

        payload: Dict[str, Any] = {"source": source}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter

        return await self.client._request("POST", "/files/bulkDelete", json=payload)

# ---------------------------
# FIRMWARES
# ---------------------------
class FirmwareAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        with_meta: bool = False,
        **filters,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List firmware files.

        Additional filters can be supplied as keyword arguments and will be
        forwarded to :meth:`FileAPI.list`.
        """
        return await self.client.files.list(
            type="firmware", with_meta=with_meta, **filters
        )

    async def upload(
        self,
        file_path: str,
        description: str = "",
        company_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        company_ids = [company_id] if company_id is not None else None
        return await self.client.files.upload(
            file_path,
            file_type="firmware",
            description=description,
            company_ids=company_ids,
        )

    async def download(self, file_id: int) -> bytes:
        return await self.client.files.download(file_id)

    async def delete(self, file_id: int) -> None:
        await self.client.files.delete(file_id)

    async def get(self, file_id: int) -> Dict[str, Any]:
        return await self.client.files.get(file_id)

# ---------------------------
# CONFIGURATIONS (над файловим API)
# ---------------------------
class ConfigurationAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        with_meta: bool = False,
        **filters,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List configuration files with optional metadata.

        Additional filters can be supplied as keyword arguments and will be
        forwarded to :meth:`FileAPI.list`.
        """
        return await self.client.files.list(
            type="configuration", with_meta=with_meta, **filters
        )

    async def upload(self, file_path: str, description: str = "", company_id: Optional[int] = None) -> Dict[str, Any]:
        company_ids = [company_id] if company_id is not None else None
        return await self.client.files.upload(
            file_path,
            file_type="configuration",
            description=description,
            company_ids=company_ids,
        )

    async def download(self, file_id: int) -> bytes:
        return await self.client.files.download(file_id)

    async def delete(self, file_id: int) -> None:
        await self.client.files.delete(file_id)

    async def get(self, file_id: int) -> Dict[str, Any]:
        return await self.client.files.get(file_id)

# ---------------------------
# COMPANIES
# ---------------------------
class CompanyAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        page: int = 1,
        per_page: int = 25,
        sort: Optional[Union[str, Sequence[str]]] = None,
        order: Optional[Union[str, Sequence[str]]] = None,
        with_meta: bool = False,
        **filters,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List companies with optional filters and pagination.

        Additional filters can be supplied as keyword arguments. Sequence
        values are serialized as repeated query parameters.
        """

        params = _build_params(
            page=page, per_page=per_page, sort=sort, order=order, **filters
        )

        kwargs: Dict[str, Any] = {"params": params}
        if with_meta:
            kwargs["unwrap"] = False
        data = await self.client._request("GET", "/companies", **kwargs)
        data = _unwrap_data(data, aggressive=not with_meta)
        if with_meta:
            return data or {}
        return data or []

    async def get(self, company_id: int) -> Dict[str, Any]:
        data = await self.client._request("GET", f"/companies/{company_id}")
        return _unwrap_data(data, aggressive=True)

    async def get_stats(
        self,
        company_id: Optional[int] = None,
        company_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Return statistics for one or more companies.

        Parameters
        ----------
        company_id: Optional[int]
            Single company ID (kept for backwards compatibility).
        company_ids: Optional[List[int]]
            List of company IDs. Values are serialized as repeated
            ``company_id`` query parameters per API contract.
        """

        params: List[tuple] = []
        if company_id is not None:
            params.append(("company_id", company_id))
        if company_ids:
            params.extend(("company_id", cid) for cid in company_ids)

        return await self.client._request(
            "GET", "/companies/stats", params=params or None
        )

    async def create(self, name: str, company_id: int) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"name": name, "company_id": company_id}
        return await self.client._request("POST", "/companies", json=payload)

    async def update(self, company_id: int, **fields) -> Dict[str, Any]:
        """Partially update company fields via ``PATCH``.

        Use :meth:`replace` for full company replacement using ``PUT``.
        """
        if not fields:
            return await self.get(company_id)
        return await self.client._request(
            "PATCH", f"/companies/{company_id}", json=fields
        )

    async def replace(self, company_id: int, **fields) -> Dict[str, Any]:
        """Replace the company resource using ``PUT``."""
        if not fields:
            return await self.get(company_id)
        return await self.client._request(
            "PUT", f"/companies/{company_id}", json=fields
        )

    async def delete(self, company_id: int) -> None:
        await self.client._request("DELETE", f"/companies/{company_id}")

    async def filter_list(
        self,
        field: str,
        company_id: Optional[Union[int, List[int]]] = None,
    ) -> Dict[str, Any]:
        params: List[tuple] = [("field", field)]
        if company_id is not None:
            if isinstance(company_id, list):
                params.extend(("company_id", cid) for cid in company_id)
            else:
                params.append(("company_id", company_id))
        return await self.client._request(
            "GET", "/companies/filterList", params=params
        )

    async def bulk_delete(
        self,
        source: str,
        id_list: Optional[List[int]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if source not in {"id_list", "filter"}:
            raise ValueError("source must be 'id_list' or 'filter'")
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")

        payload: Dict[str, Any] = {"source": source}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter
        return await self.client._request(
            "POST", "/companies/bulkDelete", json=payload
        )

    async def merge(self, source: int, target: int) -> Dict[str, Any]:
        payload = {"source": source, "target": target}
        return await self.client._request(
            "POST", "/companies/merge", json=payload
        )

    async def inheritance(self, company_id: int) -> Dict[str, Any]:
        return await self.client._request(
            "GET", f"/companies/{company_id}/inheritance"
        )

# ---------------------------
# USERS
# ---------------------------
class UserAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list(
        self,
        page: int = 1,
        per_page: int = 25,
        sort: Optional[Union[str, Sequence[str]]] = None,
        order: Optional[Union[str, Sequence[str]]] = None,
        with_meta: bool = False,
        **filters,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """List users with optional filters and pagination.

        Additional filters can be provided as keyword arguments. Sequence
        values are serialized as repeated query parameters.
        """
        params = _build_params(
            page=page, per_page=per_page, sort=sort, order=order, **filters
        )
        kwargs: Dict[str, Any] = {"params": params}
        if with_meta:
            kwargs["unwrap"] = False
        data = await self.client._request("GET", "/users", **kwargs)
        data = _unwrap_data(data, aggressive=not with_meta)
        if with_meta:
            return data or {}
        return data or []

    async def filter_list(
        self,
        field: str,
        company_id: Optional[Union[int, List[int]]] = None,
    ) -> Dict[str, Any]:
        params: List[tuple] = [("field", field)]
        if company_id is not None:
            if isinstance(company_id, Sequence) and not isinstance(company_id, (str, bytes, bytearray)):
                params.extend(("company_id", cid) for cid in company_id)
            else:
                params.append(("company_id", company_id))
        return await self.client._request("GET", "/users/filterList", params=params)

    async def get(self, user_id: Union[int, str]) -> Dict[str, Any]:
        user_id_str = str(user_id)
        data = await self.client._request("GET", f"/users/{user_id_str}")
        return _unwrap_data(data, aggressive=True)

    async def update(self, user_id: Union[int, str], **fields) -> Dict[str, Any]:
        """Partially update user fields via ``PATCH``.

        Use :meth:`replace` for full user replacement using ``PUT``.
        """
        user_id_str = str(user_id)
        if not fields:
            return await self.get(user_id_str)
        return await self.client._request("PATCH", f"/users/{user_id_str}", json=fields)

    async def replace(self, user_id: Union[int, str], **fields) -> Dict[str, Any]:
        """Replace the user resource using ``PUT``."""
        user_id_str = str(user_id)
        if not fields:
            return await self.get(user_id_str)
        return await self.client._request("PUT", f"/users/{user_id_str}", json=fields)

    async def delete(self, user_id: Union[int, str]) -> None:
        user_id_str = str(user_id)
        await self.client._request("DELETE", f"/users/{user_id_str}")

    async def bulk_delete(
        self,
        source: str,
        id_list: Optional[List[Union[int, str]]] = None,
        filter: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if source not in {"id_list", "filter"}:
            raise ValueError("source must be 'id_list' or 'filter'")
        if source == "id_list" and not id_list:
            raise ValueError("id_list must be provided when source='id_list'")
        if source == "filter" and filter is None:
            raise ValueError("filter must be provided when source='filter'")

        payload: Dict[str, Any] = {"source": source}
        if id_list is not None:
            payload["id_list"] = id_list
        if filter is not None:
            payload["filter"] = filter
        return await self.client._request("POST", "/users/bulkDelete", json=payload)

# ---------------------------
# BACKGROUND ACTIONS
# ---------------------------
class BackgroundActionAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def get(self, background_action_id: str) -> Dict[str, Any]:
        """Fetch a background action by its identifier.

        The backend may wrap the payload in ``{"data": {...}}``.
        Any unrecognised structure is returned as-is.
        """
        data = await self.client._request(
            "GET", f"/backgroundActions/{background_action_id}"
        )
        return _unwrap_data(data, aggressive=True)

# ---------------------------
# CAN ADAPTERS
# ---------------------------
class CanAdapterAPI:
    def __init__(self, client: FotaClient):
        self.client = client

    async def list_vehicles(self) -> List[Dict[str, Any]]:
        """
        Повертає повний список підтримуваних авто.
        Підтримує такі форми відповіді бекенду:
        - {"vehicles": [...]}
        - {"data": [...]}
        - {"data": {"vehicles": [...]}}
        - [...]
        """
        data = await self.client._request("GET", "/canAdapters/vehicles")

        if isinstance(data, dict):
            if "vehicles" in data and isinstance(data["vehicles"], list):
                return data["vehicles"]
            if "data" in data and isinstance(data["data"], list):
                return data["data"]
            if "data" in data and isinstance(data["data"], dict) and isinstance(data["data"].get("vehicles"), list):
                return data["data"]["vehicles"]

        if isinstance(data, list):
            return data

        return []

# ---------------------------
# ТЕСТ У МОДУЛІ
# ---------------------------
if __name__ == "__main__":
    async def main():
        # !!! На час відладки вкажіть ваш токен тут:
        TEST_TOKEN = "8806|rFq0ZV5dtioKwdx9bXSEGmxMsfEMX3UKSat41NhS"

        async with FotaClient(token=TEST_TOKEN) as client:
            print("[TEST] Base URL:", client.base_url)

            # 1) Перевіримо доступ до каталогу CAN (smoke‑test токена)
            try:
                vehicles = await client.can_adapters.list_vehicles()
                print(f"[TEST] CAN vehicles fetched: {len(vehicles)} items")
            except FotaWebApiError as e:
                print("[TEST][ERROR] canAdapters/vehicles:", e)
                raise

            # 2) Візьмемо перший доступний пристрій, щоб дізнатись company_id
            devices = await client.devices.list(per_page=1)
            if not devices:
                print("[TEST] No devices visible for this token → немає як отримати company_id")
                return
            dev = devices[0]
            print("[TEST] Sample device:", dev)

            # Витяг company_id з відповіді пристрою
            company_id = (
                dev.get("company_id")
                or (dev.get("company") or {}).get("id")
                or dev.get("companyId")
                or dev.get("companyID")
            )
            if not company_id:
                print("[TEST] Could not detect company_id from device payload")
                return

            # 3) Тестуємо /companies/stats саме з цим company_id
            stats = await client.companies.get_stats(company_id=int(company_id))
            print("[TEST] Company stats:", stats)

    asyncio.run(main())
