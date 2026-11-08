"""Create a small valid WAV file for a zero-cost smoke test."""

import math
import struct
import wave
from pathlib import Path


output = Path(__file__).parents[1] / "sample_data" / "sample.wav"
output.parent.mkdir(exist_ok=True)
rate = 16_000
duration = 65
with wave.open(str(output), "wb") as wav:
    wav.setparams((1, 2, rate, rate * duration, "NONE", "not compressed"))
    for index in range(rate * duration):
        value = int(5000 * math.sin(2 * math.pi * 440 * index / rate))
        wav.writeframesraw(struct.pack("<h", value))
print(output)
