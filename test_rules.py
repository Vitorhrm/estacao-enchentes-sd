import unittest
from system import decision, signed, valid, quorum

class Rules(unittest.TestCase):
    def test_interlock(self):
        wet = {"level_cm": 150, "rain_mm_h": 45, "gate_open": False}
        self.assertFalse(decision("MANUAL_ON", wet, False))
        self.assertTrue(decision("AUTO", dict(wet, gate_open=True), False))
        self.assertTrue(decision("AUTO", {"level_cm": 75, "rain_mm_h": 0, "gate_open": True}, True))
        self.assertFalse(decision("AUTO", {"level_cm": 60, "rain_mm_h": 0, "gate_open": True}, True))

    def test_signature(self):
        message = signed("key", {"id": "a", "pump": True})
        self.assertTrue(valid("key", message))
        self.assertFalse(valid("key", dict(message, pump=False)))

    def test_quorum(self):
        self.assertIsNone(quorum({"a": True, "b": True, "c": False, "d": False}))
        self.assertIs(quorum({"a": True, "b": False, "c": True, "d": True}), True)
        self.assertIs(quorum({"a": False, "b": False, "c": False}), False)

if __name__ == "__main__":
    unittest.main()
