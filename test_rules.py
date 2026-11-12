import unittest
from system import decide_pump_state, sign_message, verify_message, find_quorum

class Rules(unittest.TestCase):
    def test_interlock(self):
        """Verifica a trava da comporta e os limiares de partida e parada."""
        wet = {"level_cm": 150, "rain_mm_h": 45, "gate_open": False}
        self.assertFalse(decide_pump_state("MANUAL_ON", wet, False))
        self.assertTrue(decide_pump_state("AUTO", dict(wet, gate_open=True), False))
        self.assertTrue(decide_pump_state("AUTO", {"level_cm": 75, "rain_mm_h": 0, "gate_open": True}, True))
        self.assertFalse(decide_pump_state("AUTO", {"level_cm": 60, "rain_mm_h": 0, "gate_open": True}, True))

    def test_signature(self):
        """Confirma que alterar o conteúdo invalida a autenticação."""
        message = sign_message("key", {"id": "a", "pump": True})
        self.assertTrue(verify_message("key", message))
        self.assertFalse(verify_message("key", dict(message, pump=False)))

    def test_quorum(self):
        """Exige três votos iguais, incluindo quando a decisão é desligar."""
        self.assertIsNone(find_quorum({"a": True, "b": True, "c": False, "d": False}))
        self.assertIs(find_quorum({"a": True, "b": False, "c": True, "d": True}), True)
        self.assertIs(find_quorum({"a": False, "b": False, "c": False}), False)

if __name__ == "__main__":
    unittest.main()
