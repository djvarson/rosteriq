"""One user's notification preferences must never change anyone else's.

_merge_with_defaults shallow-copied DEFAULT_PREFS and then .update()d the
nested dicts, rewriting the class-level defaults: after one user turned SMS
on, every user without saved preferences had SMS on too."""

from rosteriq.services.notification_preferences import NotificationPreferences


def test_merging_one_users_prefs_leaves_defaults_alone():
    svc = NotificationPreferences()
    before = svc._merge_with_defaults({})
    svc._merge_with_defaults({"channels": {"sms": True, "email": False},
                              "quiet_hours": {"start": "20:00", "end": "09:00"}})
    assert NotificationPreferences.DEFAULT_PREFS["channels"]["sms"] is False
    assert NotificationPreferences.DEFAULT_PREFS["quiet_hours"]["start"] == "22:00"
    assert svc._merge_with_defaults({}) == before
