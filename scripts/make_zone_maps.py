"""
Build the zone-page maps from enchantment-permit-zones-map.png.

For each zone, the zone keeps its original colours and the rest of the map is
washed out (desaturated and lightened, still readable). Each zone's outline is
found by flood-filling from a point inside it, with the map's red boundary
lines as walls. The main trails and their trailheads are drawn on top of
every map. Writes static/maps/<zone>.webp and <zone>-900.webp.

Run from the repo root after the source map changes:
    python scripts/make_zone_maps.py [path/to/map.png]
Needs Pillow and numpy (not needed by the web app).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont

BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE = BASE_DIR / "enchantment-permit-zones-map.png"
OUT_DIR = BASE_DIR / "static" / "maps"

# Zone slug -> a pixel inside the zone on the 1638x1062 source map.
SEEDS = {
    "core": (1050, 650),
    "colchuck": (700, 750),
    "snow": (1330, 820),
    "eightmile": (300, 520),
    "stuart": (300, 800),
}
# Main trails as pixel paths on the source map, traced by eye from the faint
# dashed trails and trail numbers on the topo, so they are approximate.
TRAILS = {
    # Stuart Lake trailhead -> Mountaineer Creek -> Colchuck Lake (1599A)
    # -> Aasgard Pass -> upper and lower Enchantment basins -> Lake Viviane
    # -> Snow Lakes -> Nada Lake -> Snow Lakes trailhead (1553).
    "through-hike": [
        (778, 432), (768, 450), (763, 480), (760, 510), (752, 540), (742, 565),
        (732, 590), (725, 602), (705, 618), (680, 635), (655, 652),
        (645, 668), (638, 688), (645, 705), (662, 712), (675, 728), (678, 745),
        (668, 756), (662, 772), (663, 792), (673, 810), (684, 816),
        (700, 826), (720, 840), (740, 858), (758, 875), (778, 890),
        (800, 902), (825, 908), (855, 905), (885, 895), (905, 888), (925, 892),
        (950, 902), (962, 915), (982, 908), (1000, 912), (1012, 895),
        (1025, 882), (1042, 882), (1062, 890), (1085, 898), (1110, 900),
        (1125, 896), (1150, 905), (1180, 905), (1210, 890), (1235, 862),
        (1246, 840), (1258, 825), (1275, 800), (1290, 780), (1302, 760),
        (1320, 740), (1338, 725), (1360, 700), (1385, 668), (1410, 632),
        (1430, 610), (1440, 575), (1442, 540), (1450, 500), (1462, 470),
        (1478, 440), (1490, 400), (1495, 360), (1503, 320), (1500, 292),
        (1490, 278),
    ],
    # Branches off the through-hike route at the Colchuck junction (1599).
    "stuart-lake": [
        (655, 652), (625, 666), (600, 676), (560, 691), (520, 708), (490, 720),
        (460, 727), (440, 729),
    ],
    # Eightmile trailhead -> Little Eightmile Lake -> Eightmile Lake (1552).
    "eightmile-lake": [
        (828, 322), (800, 330), (760, 337), (740, 348), (728, 362), (712, 375),
        (700, 390), (682, 408), (665, 420), (648, 433), (628, 441), (608, 451),
        (590, 462), (578, 470), (555, 480),
        (538, 490), (525, 498),
    ],
}
# Trailhead label -> marker position and label anchor ("l" = text left of it).
TRAILHEADS = {
    "Stuart Lake / Colchuck TH": ((778, 432), "r"),
    "Eightmile TH": ((828, 322), "r"),
    "Snow Lakes TH": ((1490, 278), "l"),
}
# Lake label -> a point on the trail at the lake, marked with a smaller dot,
# and which side of it the label goes, chosen to keep the text off the lake.
LAKES = {
    "Stuart Lake": ((440, 729), "a"),
    "Eightmile Lake": ((525, 498), "b"),
    "Colchuck Lake": ((662, 772), "l"),
    # On the Colchuck/Core boundary, where the trail crosses it.
    "Aasgard Pass": ((734, 853), "ar"),  # above-right: the trail climbs in from the left
    "Perfection Lake":((945, 899), "bl"),  # below-left: the trail drops away to the right
    "Snow Lakes": ((1180, 905), "b"),
}
TRAIL_COLOR = (255, 212, 0)
TRAIL_EDGE = (35, 35, 35)
SCALE = 2  # draw trails at 2x and shrink, for smooth edges

SMALL_WIDTH = 900
SATURATION = 0.15  # left outside the zone
WASH = 0.45        # blend toward white outside the zone


def red_walls(img: Image.Image) -> Image.Image:
    a = np.asarray(img).astype(int)
    r, g, b = a[..., 0], a[..., 1], a[..., 2]
    red = (r > 170) & (g < 110) & (b < 110) & (r - g > 90)
    # Thicken the lines so the fill cannot leak through gaps in them.
    return Image.fromarray((red * 255).astype(np.uint8)).filter(ImageFilter.MaxFilter(9))


def zone_mask(walls: Image.Image, seed) -> Image.Image:
    fill = walls.copy()
    ImageDraw.floodfill(fill, seed, 128)
    region = Image.fromarray(((np.asarray(fill) == 128) * 255).astype(np.uint8))
    # Close the small holes left by red map symbols, then grow over the
    # zone's own boundary line so the outline stays in full colour.
    region = region.filter(ImageFilter.MaxFilter(15)).filter(ImageFilter.MinFilter(15))
    return region.filter(ImageFilter.MaxFilter(11)).filter(ImageFilter.GaussianBlur(2))


def washed(img: Image.Image) -> Image.Image:
    grey = img.convert("L").convert("RGB")
    muted = Image.blend(grey, img, SATURATION)
    return Image.blend(muted, Image.new("RGB", img.size, "white"), WASH)


def _font(size: int):
    for name in ("arialbd.ttf", "DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            pass
    return ImageFont.load_default(size)


def trail_overlay(size) -> Image.Image:
    """The trails, trailheads and lake labels on a transparent layer the size
    of the map."""
    layer = Image.new("RGBA", (size[0] * SCALE, size[1] * SCALE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)

    def pts(path):
        return [(x * SCALE, y * SCALE) for x, y in path]

    for path in TRAILS.values():
        draw.line(pts(path), fill=TRAIL_EDGE, width=7 * SCALE, joint="curve")
    for path in TRAILS.values():
        draw.line(pts(path), fill=TRAIL_COLOR, width=4 * SCALE, joint="curve")
    font = _font(15 * SCALE)

    def marker(label, x, y, side, radius):
        x, y, r = x * SCALE, y * SCALE, radius * SCALE
        draw.ellipse((x - r, y - r, x + r, y + r), fill=TRAIL_COLOR, outline=TRAIL_EDGE, width=2 * SCALE)
        gap = r + 4 * SCALE
        pos, anchor = {"r": ((x + gap, y), "lm"), "l": ((x - gap, y), "rm"),
                       "a": ((x, y - gap), "mb"), "b": ((x, y + gap), "mt"),
                       "bl": ((x + r, y + gap + 6 * SCALE), "rt"),
                       "ar": ((x + r, y - r - 2 * SCALE), "ld")}[side]
        draw.text(pos, label, font=font, fill=TRAIL_EDGE, anchor=anchor,
                  stroke_width=3 * SCALE, stroke_fill="white")

    for label, ((x, y), side) in LAKES.items():
        marker(label, x, y, side, 5)
    for label, ((x, y), side) in TRAILHEADS.items():
        marker(label, x, y, side, 7)
    return layer.resize(size, Image.LANCZOS)


def main() -> None:
    source = Path(sys.argv[1]) if len(sys.argv) > 1 else SOURCE
    img = Image.open(source).convert("RGB")
    walls, background = red_walls(img), washed(img)
    trails = trail_overlay(img.size)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for slug, seed in SEEDS.items():
        mask = zone_mask(walls, seed)
        coverage = (np.asarray(mask) > 127).mean()
        if not 0.02 < coverage < 0.25:
            raise SystemExit(f"{slug}: mask covers {coverage:.0%} of the map; check the seed point")
        out = Image.composite(img, background, mask)
        out.paste(trails, (0, 0), trails)
        out.save(OUT_DIR / f"{slug}.webp", quality=80, method=6)
        small = out.resize((SMALL_WIDTH, round(out.height * SMALL_WIDTH / out.width)), Image.LANCZOS)
        small.save(OUT_DIR / f"{slug}-{SMALL_WIDTH}.webp", quality=80, method=6)
        print(f"{slug}: {coverage:.1%} of map")


if __name__ == "__main__":
    main()
