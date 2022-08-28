from dataclasses import dataclass, field
from faker import Faker

faker = Faker()

@dataclass
class ClientA:
    client: str ='client_a'
    event_type: str = 'sales'
    value: str = field(default_factory=faker.pricetag)
    credit_card_number: str = field(default_factory=faker.credit_card_number)

@dataclass
class ClientB:
    client: str ='client_b'
    event_type: str = 'lead'
    first_name: str = field(default_factory=faker.first_name)
    last_name: str = field(default_factory=faker.last_name)

@dataclass
class ClientC:
    client: str ='client_c'
    event_type: str = 'geolocation'
    street_address: str = field(default_factory=faker.street_address)
    city: str = field(default_factory=faker.city)
