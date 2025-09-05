Teltonika FOTA Web API (Async Python Client)

Unofficial async client for the Teltonika FOTA Web API, written by Max Ereschenko.
This is not an official Teltonika document or library.

What it does
	•	Auth via Authorization: Bearer <token> + required User-Agent
	•	Covers core resources: Devices, Tasks, Files, Groups, Companies, Users, Batches, Background Actions, CAN Adapters
	•	Bulk operations (tri-source: id_list / filter)
	•	Handles rate limits (429 + Retry-After) with exponential backoff
	•	Clean JSON unwrapping and optional pagination metadata

Requirements
	•	Python 3.9+
	•	aiohttp

pip install aiohttp

Quick Start
```python
import os
import asyncio
from your_file import FotaClient

async def main():
    token = os.getenv("FOTA_TOKEN")
    if not token:
        raise RuntimeError("Set FOTA_TOKEN env var")
    async with FotaClient(token=token, user_agent="MicroTronic FotaClient/1.0") as client:
        devices = await client.devices.list()
        print("Devices:", len(devices))

asyncio.run(main())
```
Notes
	•	Set a valid User-Agent string in the form: CompanyName AppName/Version.
	•	The client writes debug logs to fota_api_debug.log and masks your token.
	•	For BLE/BlueNRG uploads use file types ble_fw or blue_nrg.

Disclaimer

This project is maintained by Max Ereschenko and is not affiliated with or endorsed by Teltonika. Use at your own risk.
