"""
Harmonic Error Notifier - Sends a Glue notification once daily if Harmonic API calls fail.
"""
from datetime import datetime


class HarmonicErrorNotifier:
    """Handles sending Glue notifications for Harmonic API failures (once per day)"""

    GLUE_THREAD_ID = "thr_37BrUUDCQUG1ZVVwKdm5Oi1Un1M"
    TOKEN_UPDATE_THREAD_URL = "https://app.glue.ai/inbox/thr_37KZXd2r1F8J4LOuBWnpROWRVs8"

    # Harmonic-side infrastructure failures. These surface as GraphQL errors
    # but are NOT auth problems (the token is fine) — refreshing it does
    # nothing. Callers keyword-match on "token"/"invalid"/etc., so a transient
    # 5xx / connection error from Harmonic's own backend can be misrouted to
    # notify_auth_failure(). We catch that here and downgrade to a plain API
    # error so no one is told to refresh a perfectly valid token.
    SERVER_ERROR_KEYWORDS = [
        "internal_server_error", "econnrefused", "etimedout", "econnreset",
        "network timeout", "socket hang up", "bad gateway",
        "service unavailable", "gateway timeout", "502", "503", "504",
    ]

    @classmethod
    def _looks_like_server_error(cls, error_details: str) -> bool:
        """True if the error text clearly indicates a Harmonic-side outage."""
        if not error_details:
            return False
        low = error_details.lower()
        return any(kw in low for kw in cls.SERVER_ERROR_KEYWORDS)

    def __init__(self):
        self.messenger = None
        try:
            from glue.send_message import SendMessage
            self.messenger = SendMessage()
        except Exception as e:
            print(f"Warning: Could not initialize Glue messenger: {e}")

    def notify_auth_failure(self, error_details: str = None) -> bool:
        """
        Send a notification about Harmonic authentication failure.
        Uses uniqueBy with today's date to ensure only one notification per day.

        Args:
            error_details: Optional error message details

        Returns:
            True if notification was sent/attempted, False otherwise
        """
        # Don't cry "refresh the token" for a Harmonic-side outage. Callers'
        # broad keyword matching ("token"/"invalid"/...) can misclassify a
        # transient 5xx / connection error as an auth failure; route those to
        # the neutral API-error notifier instead.
        if self._looks_like_server_error(error_details):
            return self.notify_api_error("GraphQL Error", error_details)

        if not self.messenger:
            print("Harmonic auth error detected but Glue messenger not available")
            return False

        today = datetime.now().strftime("%Y-%m-%d")
        unique_key = f"harmonic_auth_error_{today}"

        message = f"Harmonic API authentication failed. The Bearer token may need to be refreshed.\n\nRefresh at: https://console.harmonic.ai\n\nFYI: Post the updated token to the [Token Update Thread]({self.TOKEN_UPDATE_THREAD_URL})"

        if error_details:
            message += f"\n\nError: {error_details}"

        try:
            self.messenger.send_chat_message(
                target=self.GLUE_THREAD_ID,
                text=message,
                unique_by=unique_key
            )
            print(f"Harmonic error notification sent to Glue (unique_by: {unique_key})")
            return True
        except Exception as e:
            print(f"Failed to send Harmonic error notification: {e}")
            return False

    def notify_api_error(self, error_type: str, error_details: str = None) -> bool:
        """
        Send a notification about a general Harmonic API error.
        Uses uniqueBy with today's date to ensure only one notification per day.

        Args:
            error_type: Type of error (e.g., "rate_limit", "server_error")
            error_details: Optional error message details

        Returns:
            True if notification was sent/attempted, False otherwise
        """
        if not self.messenger:
            print("Harmonic API error detected but Glue messenger not available")
            return False

        today = datetime.now().strftime("%Y-%m-%d")
        unique_key = f"harmonic_api_error_{today}"

        message = f"Harmonic API error: {error_type}"

        if error_details:
            message += f"\n\nDetails: {error_details}"

        try:
            self.messenger.send_chat_message(
                target=self.GLUE_THREAD_ID,
                text=message,
                unique_by=unique_key
            )
            print(f"Harmonic API error notification sent to Glue (unique_by: {unique_key})")
            return True
        except Exception as e:
            print(f"Failed to send Harmonic API error notification: {e}")
            return False
