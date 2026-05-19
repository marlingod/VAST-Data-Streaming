"""
Fraud pattern generators for the VAST Data fraud-detection demo.

Each pattern class encapsulates a single fraud strategy.  The generator
picks a pattern at random (or a specific one via --inject) and calls
``generate(customer_profile)`` to produce one or more fraudulent
transaction dicts that look realistic but contain the tell-tale
signature of the attack.

CustomerProfile / CustomerPool keep per-customer state so that
*legitimate* transactions stay behaviorally consistent while fraud
transactions deliberately break that consistency.
"""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List

from faker import Faker

# ---------------------------------------------------------------------------
# Realistic city list with lat/lon (used for geographic-impossibility fraud
# and for assigning customer home cities).
# ---------------------------------------------------------------------------
CITIES = [
    ("New York", 40.7128, -74.0060),
    ("Los Angeles", 33.9425, -118.4081),
    ("Chicago", 41.8781, -87.6298),
    ("Houston", 29.7604, -95.3698),
    ("Phoenix", 33.4484, -112.0740),
    ("Philadelphia", 39.9526, -75.1652),
    ("San Antonio", 29.4241, -98.4936),
    ("San Diego", 32.7157, -117.1611),
    ("Dallas", 32.7767, -96.7970),
    ("San Jose", 37.3382, -121.8863),
    ("Austin", 30.2672, -97.7431),
    ("Jacksonville", 30.3322, -81.6557),
    ("Denver", 39.7392, -104.9903),
    ("Seattle", 47.6062, -122.3321),
    ("Boston", 42.3601, -71.0589),
    ("Miami", 25.7617, -80.1918),
    ("Atlanta", 33.7490, -84.3880),
    ("Minneapolis", 44.9778, -93.2650),
    ("Portland", 45.5152, -122.6784),
    ("Las Vegas", 36.1699, -115.1398),
    ("Detroit", 42.3314, -83.0458),
    ("Nashville", 36.1627, -86.7816),
    ("Salt Lake City", 40.7608, -111.8910),
    ("Kansas City", 39.0997, -94.5786),
    ("London", 51.5074, -0.1278),
    ("Paris", 48.8566, 2.3522),
    ("Tokyo", 35.6762, 139.6503),
    ("Sydney", -33.8688, 151.2093),
    ("Toronto", 43.6532, -79.3832),
    ("Berlin", 52.5200, 13.4050),
]

# Merchant categories used across the demo
MERCHANT_CATEGORIES = [
    "grocery", "gas_station", "restaurant", "electronics",
    "clothing", "travel", "entertainment", "pharmacy",
    "home_improvement", "online_retail",
]

# ---------------------------------------------------------------------------
# Fixed merchant pool — realistic, reusable merchants across all customers
# ---------------------------------------------------------------------------
MERCHANTS = [
    # Grocery
    ("MER-WALMART-001", "grocery"), ("MER-WALMART-002", "grocery"), ("MER-WALMART-003", "grocery"),
    ("MER-KROGER-001", "grocery"), ("MER-KROGER-002", "grocery"),
    ("MER-WHOLEFDS-001", "grocery"), ("MER-WHOLEFDS-002", "grocery"),
    ("MER-COSTCO-001", "grocery"), ("MER-COSTCO-002", "grocery"),
    ("MER-TRADERJOE-001", "grocery"), ("MER-ALDI-001", "grocery"),
    # Gas
    ("MER-SHELL-001", "gas_station"), ("MER-SHELL-002", "gas_station"),
    ("MER-CHEVRON-001", "gas_station"), ("MER-EXXON-001", "gas_station"),
    ("MER-BP-001", "gas_station"),
    # Restaurant
    ("MER-STARBUCKS-001", "restaurant"), ("MER-STARBUCKS-002", "restaurant"), ("MER-STARBUCKS-003", "restaurant"),
    ("MER-MCDONALDS-001", "restaurant"), ("MER-MCDONALDS-002", "restaurant"),
    ("MER-CHIPOTLE-001", "restaurant"), ("MER-CHIPOTLE-002", "restaurant"),
    ("MER-DOMINOS-001", "restaurant"), ("MER-SUBWAY-001", "restaurant"),
    ("MER-OLIVEGARDEN-001", "restaurant"),
    # Electronics
    ("MER-BESTBUY-001", "electronics"), ("MER-BESTBUY-002", "electronics"),
    ("MER-APPLE-001", "electronics"), ("MER-APPLE-002", "electronics"),
    ("MER-AMAZON-001", "electronics"), ("MER-AMAZON-002", "electronics"), ("MER-AMAZON-003", "electronics"),
    # Clothing
    ("MER-TARGET-001", "clothing"), ("MER-TARGET-002", "clothing"),
    ("MER-NORDSTROM-001", "clothing"), ("MER-MACYS-001", "clothing"),
    ("MER-GAP-001", "clothing"), ("MER-HM-001", "clothing"),
    # Travel
    ("MER-DELTA-001", "travel"), ("MER-UNITED-001", "travel"),
    ("MER-MARRIOTT-001", "travel"), ("MER-HILTON-001", "travel"),
    ("MER-HERTZ-001", "travel"), ("MER-UBER-001", "travel"), ("MER-UBER-002", "travel"),
    ("MER-LYFT-001", "travel"),
    # Entertainment
    ("MER-NETFLIX-001", "entertainment"), ("MER-SPOTIFY-001", "entertainment"),
    ("MER-AMC-001", "entertainment"), ("MER-DISNEY-001", "entertainment"),
    # Pharmacy
    ("MER-CVS-001", "pharmacy"), ("MER-CVS-002", "pharmacy"),
    ("MER-WALGREENS-001", "pharmacy"), ("MER-WALGREENS-002", "pharmacy"),
    # Home
    ("MER-HOMEDEPOT-001", "home_improvement"), ("MER-HOMEDEPOT-002", "home_improvement"),
    ("MER-LOWES-001", "home_improvement"),
    # Online retail
    ("MER-AMAZON-RETAIL-001", "online_retail"), ("MER-AMAZON-RETAIL-002", "online_retail"),
    ("MER-EBAY-001", "online_retail"), ("MER-ETSY-001", "online_retail"),
    ("MER-SHOPIFY-001", "online_retail"),
]

MERCHANT_IDS = [m[0] for m in MERCHANTS]
MERCHANT_CATEGORY_MAP = {m[0]: m[1] for m in MERCHANTS}

# A fixed set of "known fraud ring" merchant IDs
FRAUD_RING_MERCHANTS = [
    "MER-FR-SHELL-001", "MER-FR-QUICKMART-001", "MER-FR-LUXGOODS-001",
    "MER-FR-CRYPTOATM-001", "MER-FR-GIFTCARD-001",
]

CHANNELS = ["online", "pos", "atm", "mobile"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in km between two lat/lon points."""
    R = 6371.0  # Earth radius in kilometres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _iso_now() -> str:
    """UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _txn_id() -> str:
    """Generate a unique transaction ID."""
    return f"TXN-{uuid.uuid4().hex[:12].upper()}"


def _make_txn(
    customer: "CustomerProfile",
    *,
    amount: float,
    city: str,
    lat: float,
    lon: float,
    merchant_id: str | None = None,
    merchant_category: str | None = None,
    device: str | None = None,
    channel: str | None = None,
    timestamp: str | None = None,
    is_fraud: bool = True,
) -> dict:
    """Build a single transaction dict, filling in sensible defaults."""
    if merchant_id is None:
        merchant_id = random.choice(customer.known_merchants)
    if merchant_category is None:
        merchant_category = MERCHANT_CATEGORY_MAP.get(merchant_id, random.choice(MERCHANT_CATEGORIES))
    return {
        "transaction_id": _txn_id(),
        "timestamp": timestamp or _iso_now(),
        "card_id": customer.card_id,
        "customer_id": customer.customer_id,
        "merchant_id": merchant_id,
        "merchant_category": merchant_category,
        "amount": round(amount, 2),
        "currency": "USD",
        "location_lat": lat,
        "location_lon": lon,
        "location_city": city,
        "device_fingerprint": device or random.choice(customer.known_devices),
        "channel": channel or random.choice(CHANNELS),
        "is_fraud": is_fraud,
    }


# ---------------------------------------------------------------------------
# Customer profile & pool
# ---------------------------------------------------------------------------

@dataclass
class CustomerProfile:
    """Behavioural profile for a single synthetic customer."""

    customer_id: str
    card_id: str
    home_city: str
    home_lat: float
    home_lon: float
    avg_spend: float                        # average transaction amount
    known_devices: List[str] = field(default_factory=list)
    known_merchants: List[str] = field(default_factory=list)


class CustomerPool:
    """Generate and maintain *N* synthetic customer profiles.

    Every customer gets a consistent home city, spending baseline, set of
    known devices, and a handful of merchants they normally shop at.
    """

    def __init__(self, n: int, seed: int = 42) -> None:
        self._rng = random.Random(seed)
        self._fake = Faker()
        Faker.seed(seed)
        self.profiles: List[CustomerProfile] = [
            self._build_profile(i) for i in range(n)
        ]

    # ------------------------------------------------------------------
    def _build_profile(self, index: int) -> CustomerProfile:
        city_name, lat, lon = self._rng.choice(CITIES)
        avg_spend = round(self._rng.uniform(15.0, 500.0), 2)
        devices = [self._fake.sha1()[:16] for _ in range(self._rng.randint(1, 3))]
        # Each customer shops at 5-12 merchants from the shared pool
        merchants = self._rng.sample(MERCHANT_IDS, k=min(self._rng.randint(5, 12), len(MERCHANT_IDS)))
        return CustomerProfile(
            customer_id=f"CUST-{index:06d}",
            card_id=f"CARD-{uuid.uuid4().hex[:10].upper()}",
            home_city=city_name,
            home_lat=lat,
            home_lon=lon,
            avg_spend=avg_spend,
            known_devices=devices,
            known_merchants=merchants,
        )

    # ------------------------------------------------------------------
    def random_profile(self) -> CustomerProfile:
        """Return a random customer profile."""
        return self._rng.choice(self.profiles)


# ---------------------------------------------------------------------------
# Fraud pattern classes
# ---------------------------------------------------------------------------

class VelocityAttack:
    """10+ transactions from the same card within 60 seconds.

    Simulates a compromised card being used for rapid small purchases
    before the issuer can react.
    """

    def generate(self, customer: CustomerProfile) -> List[dict]:
        now = datetime.now(timezone.utc)
        count = random.randint(10, 18)
        txns = []
        for i in range(count):
            ts = (now + timedelta(seconds=random.uniform(0, 60))).isoformat()
            txns.append(_make_txn(
                customer,
                amount=round(random.uniform(5.0, 80.0), 2),
                city=customer.home_city,
                lat=customer.home_lat,
                lon=customer.home_lon,
                timestamp=ts,
                channel="online",
            ))
        return txns


class GeographicImpossibility:
    """Two transactions from cities >500 km apart within 5 minutes.

    The cardholder cannot physically travel that fast, indicating
    the card number was stolen and used remotely.
    """

    def generate(self, customer: CustomerProfile) -> List[dict]:
        # Pick a distant city (>500 km from the customer's home)
        distant_cities = [
            (name, lat, lon)
            for name, lat, lon in CITIES
            if _haversine_km(customer.home_lat, customer.home_lon, lat, lon) > 500
        ]
        if not distant_cities:
            # Fallback: just pick any different city
            distant_cities = [
                (name, lat, lon)
                for name, lat, lon in CITIES
                if name != customer.home_city
            ]

        far_city, far_lat, far_lon = random.choice(distant_cities)
        now = datetime.now(timezone.utc)
        ts_home = now.isoformat()
        ts_far = (now + timedelta(minutes=random.uniform(1, 4.5))).isoformat()

        return [
            _make_txn(
                customer,
                amount=round(random.uniform(20.0, 300.0), 2),
                city=customer.home_city,
                lat=customer.home_lat,
                lon=customer.home_lon,
                timestamp=ts_home,
            ),
            _make_txn(
                customer,
                amount=round(random.uniform(20.0, 300.0), 2),
                city=far_city,
                lat=far_lat,
                lon=far_lon,
                device=Faker().sha1()[:16],  # unknown device
                timestamp=ts_far,
            ),
        ]


class AmountAnomaly:
    """Single transaction >10x the customer's historical average spend."""

    def generate(self, customer: CustomerProfile) -> List[dict]:
        multiplier = random.uniform(10.0, 30.0)
        amount = round(customer.avg_spend * multiplier, 2)
        return [
            _make_txn(
                customer,
                amount=amount,
                city=customer.home_city,
                lat=customer.home_lat,
                lon=customer.home_lon,
                merchant_category="electronics",
                channel="online",
            )
        ]


class CardTesting:
    """5+ micro-transactions of $1-$2 in rapid succession.

    Fraudsters "test" stolen card numbers with tiny charges before
    making a large purchase.
    """

    def generate(self, customer: CustomerProfile) -> List[dict]:
        now = datetime.now(timezone.utc)
        count = random.randint(5, 10)
        txns = []
        for i in range(count):
            ts = (now + timedelta(seconds=i * random.uniform(2, 8))).isoformat()
            txns.append(_make_txn(
                customer,
                amount=round(random.uniform(1.00, 2.00), 2),
                city=customer.home_city,
                lat=customer.home_lat,
                lon=customer.home_lon,
                timestamp=ts,
                channel="online",
                merchant_category="online_retail",
            ))
        return txns


class FraudRing:
    """Transaction at a merchant ID that belongs to a known fraud ring.

    In real life these merchant IDs would come from an intelligence feed;
    here we use a hard-coded list.
    """

    def generate(self, customer: CustomerProfile) -> List[dict]:
        merchant = random.choice(FRAUD_RING_MERCHANTS)
        return [
            _make_txn(
                customer,
                amount=round(random.uniform(50.0, 500.0), 2),
                city=customer.home_city,
                lat=customer.home_lat,
                lon=customer.home_lon,
                merchant_id=merchant,
                merchant_category="online_retail",
                channel="online",
            )
        ]


# ---------------------------------------------------------------------------
# Registry: map CLI names to pattern classes
# ---------------------------------------------------------------------------
PATTERN_REGISTRY: dict[str, type] = {
    "velocity": VelocityAttack,
    "geo": GeographicImpossibility,
    "amount": AmountAnomaly,
    "card-testing": CardTesting,
    "fraud-ring": FraudRing,
}


def random_fraud_pattern() -> object:
    """Return an instance of a randomly chosen fraud pattern."""
    cls = random.choice(list(PATTERN_REGISTRY.values()))
    return cls()
