"""Tests for the front-desk concierge AI (Round 9)."""

from __future__ import annotations

import unittest

from rosteriq.concierge import (
    ConciergeAgent,
    ConciergeKnowledgeBase,
    FAQEntry,
    VenueKB,
    get_kb,
)


class KBTest(unittest.TestCase):
    def test_ensure_seeds_defaults(self):
        kb = ConciergeKnowledgeBase()
        v = kb.ensure("v1")
        self.assertGreater(len(v.faqs), 0)

    def test_register_and_get(self):
        kb = ConciergeKnowledgeBase()
        kb.register(VenueKB(venue_id="v1", venue_name="Test"))
        self.assertEqual(kb.get("v1").venue_name, "Test")

    def test_upsert_live_context_overlays(self):
        kb = ConciergeKnowledgeBase()
        kb.upsert_live_context("v1", {"open_time": "10am"})
        kb.upsert_live_context("v1", {"close_time": "11pm"})
        v = kb.get("v1")
        self.assertEqual(v.live_context["open_time"], "10am")
        self.assertEqual(v.live_context["close_time"], "11pm")

    def test_add_faqs(self):
        kb = ConciergeKnowledgeBase()
        kb.ensure("v1")
        kb.add_faqs("v1", [FAQEntry(
            question="What's special?",
            answer="Steak night.",
            keywords=["special", "tonight"],
        )])
        v = kb.get("v1")
        self.assertTrue(any(e.question == "What's special?" for e in v.faqs))


class AgentTest(unittest.TestCase):
    def setUp(self):
        self.kb = ConciergeKnowledgeBase()
        self.agent = ConciergeAgent(kb=self.kb)
        self.kb.ensure("v1", venue_name="Test Pub")

    def test_empty_query(self):
        r = self.agent.answer("v1", "")
        self.assertEqual(r.source, "fallback")

    def test_known_question_matches(self):
        # Provide live context so confidence isn't penalised by placeholders
        self.kb.upsert_live_context("v1", {"open_time": "11am", "close_time": "11pm"})
        r = self.agent.answer("v1", "What time do you open today?")
        self.assertGreater(r.confidence, 0.5)
        self.assertIsNotNone(r.matched_question)

    def test_template_substitution(self):
        self.kb.upsert_live_context("v1", {"open_time": "11am", "close_time": "midnight"})
        r = self.agent.answer("v1", "what are your hours?")
        self.assertIn("11am", r.answer)
        self.assertIn("midnight", r.answer)

    def test_template_unsubstituted_drops_confidence(self):
        # No live_context → placeholder remains → confidence drops
        r = self.agent.answer("v1", "what are your opening hours")
        self.assertIn("{open_time}", r.answer)
        # Still answers, but with reduced confidence
        self.assertLess(r.confidence, 0.95)

    def test_unknown_question_falls_back(self):
        r = self.agent.answer("v1", "asdfqwerty zxcvbn")
        self.assertEqual(r.source, "fallback")
        self.assertTrue(r.escalate)

    def test_escalation_keyword_triggers_flag(self):
        r = self.agent.answer("v1", "I have a complaint about my meal")
        self.assertTrue(r.escalate)

    def test_function_booking_escalates(self):
        r = self.agent.answer("v1", "Can we book a function for 30?")
        self.assertTrue(r.escalate)
        self.assertIn("function", r.matched_question.lower())

    def test_kids_menu(self):
        r = self.agent.answer("v1", "do you have a kids menu")
        self.assertIn("kids menu", r.answer.lower())

    def test_dog_friendly(self):
        r = self.agent.answer("v1", "are you dog friendly")
        self.assertIn("dog", r.answer.lower())

    def test_batch_answer(self):
        out = self.agent.batch_answer("v1", ["parking?", "kids menu?"])
        self.assertEqual(len(out), 2)


class SingletonTest(unittest.TestCase):
    def test_get_kb_returns_same(self):
        a = get_kb()
        b = get_kb()
        self.assertIs(a, b)


if __name__ == "__main__":
    unittest.main()
