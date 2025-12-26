import json
import threading
import time
from datetime import datetime
from queue import Queue

import pandas as pd
import requests


class AuditQueueManager:
    """Queue manager for audit API requests with error handling and rate limiting"""

    def __init__(self, delay_seconds=30, max_queue=10):
        self.delay_seconds = delay_seconds
        self.max_queue = max_queue
        self.queue = Queue(maxsize=max_queue)
        self.results = []
        self.is_running = False
        self.is_paused = False
        self.processed_count = 0
        self.error_count = 0
        self.skip_count = 0
        self.thread = None

    def add_to_queue(self, phone_number):
        """Add phone number to queue"""
        try:
            self.queue.put(phone_number, timeout=1)
            return True
        except Exception:
            return False

    def start_processing(self, api_url, identifier_kartu, identifier_paket, username):
        """Start processing queue"""
        if not self.is_running:
            self.is_running = True
            self.is_paused = False
            self.thread = threading.Thread(
                target=self._process_queue,
                args=(api_url, identifier_kartu, identifier_paket, username),
            )
            self.thread.daemon = True
            self.thread.start()

    def pause_processing(self):
        """Pause processing"""
        self.is_paused = True

    def resume_processing(self):
        """Resume processing"""
        self.is_paused = False

    def stop_processing(self):
        """Stop processing"""
        self.is_running = False
        if self.thread:
            self.thread.join(timeout=2)

    def _process_queue(self, api_url, identifier_kartu, identifier_paket, username):
        """Internal queue processing method with error handling"""
        while self.is_running:
            if not self.is_paused and not self.queue.empty():
                try:
                    phone_number = self.queue.get(timeout=1)
                    result = self._check_single_number(
                        phone_number,
                        api_url,
                        identifier_kartu,
                        identifier_paket,
                        username,
                    )
                    self.results.append(result)

                    if result.get("status") == "success":
                        self.processed_count += 1
                    elif result.get("status") == "skipped":
                        self.skip_count += 1
                    else:
                        self.error_count += 1

                    time.sleep(self.delay_seconds)
                except Exception as e:
                    error_result = {
                        "nomor": phone_number  # type: ignore
                        if "phone_number" in locals()
                        else "unknown",
                        "error": str(e),
                        "status": "queue_error",
                    }
                    self.results.append(error_result)
                    self.error_count += 1
                finally:
                    try:
                        self.queue.task_done()
                    except Exception:
                        pass
            elif self.is_paused:
                time.sleep(1)  # Wait when paused
            elif self.queue.empty():
                time.sleep(1)  # Wait for new items
            else:
                time.sleep(0.1)  # Brief pause

    def check_api_reachability(self, api_url, username):
        """Check if API is reachable before processing"""
        try:
            # Simple ping to check if API is reachable
            test_params = {"username": username, "to": "TEST"}
            response = requests.get(api_url, params=test_params, timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def _check_single_number(
        self, phone_number, api_url, identifier_kartu, identifier_paket, username
    ):
        """Check single number via API with robust error handling"""
        # Check API reachability first
        if not self.check_api_reachability(api_url, username):
            return {
                "nomor": phone_number,
                "error": "API unreachable",
                "status": "skipped",
                "message": "Cannot reach API endpoint",
            }

        try:
            params = {"username": username, "to": phone_number}

            response = requests.get(api_url, params=params, timeout=30)

            # Skip invalid responses but continue processing
            if response.status_code != 200:
                return {
                    "nomor": phone_number,
                    "error": f"HTTP {response.status_code}",
                    "status": "skipped",
                    "message": "Invalid HTTP status",
                }

            try:
                response_data = response.json()
            except json.JSONDecodeError:
                return {
                    "nomor": phone_number,
                    "error": "Invalid JSON response",
                    "status": "skipped",
                    "message": "JSON parsing failed",
                }

            # Parse response
            parsed_data = self.parse_api_response(
                response_data, identifier_kartu, identifier_paket
            )
            parsed_data["status"] = "success"
            parsed_data["raw_response"] = response_data

            return parsed_data

        except requests.exceptions.Timeout:
            return {
                "nomor": phone_number,
                "error": "Request timeout",
                "status": "skipped",
                "message": "Request timed out after 30 seconds",
            }
        except requests.exceptions.ConnectionError:
            return {
                "nomor": phone_number,
                "error": "Connection error",
                "status": "skipped",
                "message": "Failed to connect to API",
            }
        except requests.exceptions.RequestException as e:
            return {
                "nomor": phone_number,
                "error": str(e),
                "status": "skipped",
                "message": "Request failed",
            }
        except Exception as e:
            return {
                "nomor": phone_number,
                "error": str(e),
                "status": "api_error",
                "message": "Unexpected error",
            }

    def parse_api_response(self, response_json, identifier_kartu, identifier_paket):
        """Parse API response and extract required information"""

        # Normalize MSISDN
        msisdn = response_json.get("msisdn", "")
        if isinstance(msisdn, str) and msisdn.startswith("62"):
            msisdn = "0" + msisdn[2:]

        # Extract balance
        balance = response_json.get("custbalanceinfo", "0")

        # Initialize default values
        kartu = act_kartu = end_kartu = paket = act_paket = end_paket = None

        # Parse services
        services = response_json.get("Services", []) or []
        for service in services:
            package_name = service.get("packagename", "")

            if identifier_kartu.lower() in package_name.lower():
                kartu = package_name
                act_kartu = service.get("activationdate", "")
                end_kartu = service.get("enddate", "")

            if identifier_paket.lower() in package_name.lower():
                paket = package_name
                act_paket = service.get("activationdate", "")
                end_paket = service.get("enddate", "")

        # Extract additional information
        status_info = response_json.get("statusinfo", {})
        expiry_date = status_info.get("expirydate", "")
        activation_date = status_info.get("activationdate", "")

        return {
            "nomor": msisdn,
            "kartu": kartu,
            "act_kartu": act_kartu,
            "end_kartu": end_kartu,
            "paket": paket,
            "act_paket": act_paket,
            "end_paket": end_paket,
            "balance": balance,
            "expiry_date": expiry_date,
            "activation_date": activation_date,
            "status_info": status_info,
        }

    def convert_results_to_dataframe(self, results):
        """Convert results to DataFrame with error handling"""
        df_data = []

        for result in results:
            if result.get("status") == "success":
                df_data.append({
                    "nomor": result.get("nomor", ""),
                    "kartu": result.get("kartu", ""),
                    "act_kartu": result.get("act_kartu", ""),
                    "end_kartu": result.get("end_kartu", ""),
                    "paket": result.get("paket", ""),
                    "act_paket": result.get("act_paket", ""),
                    "end_paket": result.get("end_paket", ""),
                    "balance": result.get("balance", "0"),
                })
            else:
                # Add error entries to maintain data integrity
                df_data.append({
                    "nomor": result.get("nomor", ""),
                    "kartu": "ERROR",
                    "act_kartu": "",
                    "end_kartu": "",
                    "paket": "ERROR",
                    "act_paket": "",
                    "end_paket": "",
                    "balance": result.get("error", "Unknown error"),
                })

        return pd.DataFrame(df_data)

    def save_results_to_json(self, results, filename=None):
        """Save results to JSON file. Returns filename or raises exception on failure."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"audit_results_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)

        return filename

    def create_synchronized_dataframe(self, results):
        """Create a synchronized dataframe with status labels for integration with report data"""
        df_data = []

        for result in results:
            if result.get("status") == "success":
                # Determine status based on package availability
                paket = result.get("paket", "")
                if paket and paket != "":
                    status_label = "SUKSES VALID"
                else:
                    status_label = "SUKSES WAIT"

                df_data.append({
                    "nomor": result.get("nomor", ""),
                    "tujuan": result.get("nomor", ""),  # Using nomor as tujuan for now
                    "status": 20
                    if paket and paket != ""
                    else 10,  # 20 for success, 10 for wait
                    "sn": result.get("kartu", ""),
                    "tgl_status": datetime.now().date(),
                    "jam_status": datetime.now().time(),
                    "status_label": status_label,
                    "final_status": "SUKSES PROFIT",  # Default to success
                    "act_kartu": result.get("act_kartu", ""),
                    "end_kartu": result.get("end_kartu", ""),
                    "act_paket": result.get("act_paket", ""),
                    "end_paket": result.get("end_paket", ""),
                    "balance": result.get("balance", "0"),
                    "audit_source": "api_check",
                })
            else:
                # Add error entries to maintain data integrity
                df_data.append({
                    "nomor": result.get("nomor", ""),
                    "tujuan": result.get("nomor", ""),
                    "status": 0,  # 0 for error
                    "sn": "ERROR",
                    "tgl_status": datetime.now().date(),
                    "jam_status": datetime.now().time(),
                    "status_label": "GAGAL",
                    "final_status": "GAGAL A1",
                    "act_kartu": "",
                    "end_kartu": "",
                    "act_paket": "",
                    "end_paket": "",
                    "balance": result.get("error", "Unknown error"),
                    "audit_source": "api_check",
                })

        return pd.DataFrame(df_data)

    def create_audit_report(self, results, template_name="standard"):
        """Create a formatted audit report based on template"""
        if template_name == "standard":
            # Standard template with basic metrics
            success_count = len([r for r in results if r.get("status") == "success"])
            error_count = len([
                r
                for r in results
                if r.get("status") in ["error", "api_error", "skipped"]
            ])
            total_count = len(results)

            report = {
                "report_title": "Laporan Audit API",
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "summary": {
                    "total_processed": total_count,
                    "successful_checks": success_count,
                    "failed_checks": error_count,
                    "success_rate": f"{(success_count / total_count * 100):.2f}%"
                    if total_count > 0
                    else "0%",
                },
                "details": [],
            }

            details = []
            for result in results:
                if result.get("status") == "success":
                    details.append({
                        "nomor": result.get("nomor", ""),
                        "kartu": result.get("kartu", ""),
                        "paket": result.get("paket", ""),
                        "status": "BERHASIL",
                        "balance": result.get("balance", "0"),
                    })
                else:
                    details.append({
                        "nomor": result.get("nomor", ""),
                        "kartu": "ERROR",
                        "paket": "ERROR",
                        "status": "GAGAL",
                        "balance": result.get("error", "Unknown error"),
                    })

            report["details"] = details
            return report

        elif template_name == "detailed":
            # Detailed template with more information
            report = {
                "report_title": "Laporan Audit Detail API",
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "summary": {
                    "total_processed": len(results),
                    "successful_checks": len([
                        r for r in results if r.get("status") == "success"
                    ]),
                    "api_errors": len([
                        r for r in results if r.get("status") == "api_error"
                    ]),
                    "connection_errors": len([
                        r for r in results if r.get("status") == "skipped"
                    ]),
                },
                "raw_data": results,
            }

            return report

        else:
            # Custom template
            return {
                "report_title": f"Laporan Audit {template_name}",
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data": results,
            }
