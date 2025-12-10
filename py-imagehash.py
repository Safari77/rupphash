#!/bin/python3

import sys
import os
from PIL import Image
import imagehash

def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {os.path.basename(sys.argv[0])} <image_path>")
        sys.exit(1)

    image_path = sys.argv[1]

    try:
        img = Image.open(image_path)
    except Exception as e:
        print(f"Error opening image: {e}")
        sys.exit(1)

    print(f"File: {image_path}")

    # Calculate Standard DCT pHash
    # This matches pHash.org: 32x32 -> DCT -> 8x8 -> Median -> 64-bit
    hash_obj = imagehash.phash(img, hash_size=8)

    # Get hash as a hex string and integer
    hash_hex = str(hash_obj)
    hash_int = int(hash_hex, 16)

    print(f"Standard pHash (Hex): {hash_hex}")
    print(f"Standard pHash (Bin): {bin(hash_int)[2:].zfill(64)}")

    # Calculate the hash for 0, 90, 180, and 270 degrees and find the minimum.
    hashes = []
    print("\nRotational variations:")

    for angle in [0, 90, 180, 270]:
        # Rotate image (expand=True ensures size adjusts correctly)
        # Note: imagehash might resize anyway, but this is the standard rotate logic.
        if angle == 0:
            rotated = img
        else:
            rotated = img.rotate(-angle, expand=True)

        h = imagehash.phash(rotated, hash_size=8)
        val = int(str(h), 16)
        hashes.append(val)
        print(f"Rot {angle:3}°: {h}  (Bin: {val:064b})")

    min_hash = min(hashes)
    print(f"Min Hash: {min_hash:016x}")

if __name__ == "__main__":
    main()
