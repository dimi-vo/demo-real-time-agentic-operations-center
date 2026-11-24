"""checkout's support-ticket templates and the pools that fill them (D-068).

Each template is one kind of ticket: the priority its reporter picks, its true severity, the
action a really-HIGH one expects, and a subject and description in English. Placeholders are
`str.format` names (`{order_id}`, `{time}`, ...), each filled from `POOLS` by `fill`.

The mix (spec decision 10):
- 12 P1 that are really HIGH, 4 per action. Each says many customers are affected, gives a
  start time (`{time}`) and has exactly one cue to its action: a release, update, deploy or
  new version for `ROLLBACK`; a sale, campaign, peak or rush with slowness or timeouts for
  `SCALE_OUT`; a payment provider or gateway error for `INVESTIGATE_UPSTREAM`. None is
  something no action fixes (being charged twice, say).
- 12 P1 that are really LOW: one customer, a question, a request or something cosmetic. Four
  of them shout (`shouting`): capitals, "URGENT!!!", "I am losing money". That is the
  over-prioritisation the classifier has to see through.
- 8 P2 and 8 P3, all mundane and LOW.

A description is one paragraph of one to three sentences. The ticket clock
(`opsgen/tickets.py`) picks a template that matches the priority, severity and action it drew.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass

ROLLBACK = "ROLLBACK"
SCALE_OUT = "SCALE_OUT"
INVESTIGATE_UPSTREAM = "INVESTIGATE_UPSTREAM"
ACTIONS = (ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM)

HIGH = "HIGH"
LOW = "LOW"


@dataclass(frozen=True)
class Template:
    priority: str  # P1 | P2 | P3, as the reporter set it
    true_severity: str  # HIGH | LOW
    expected_action: str | None  # one of ACTIONS for HIGH, None for LOW
    subject: str
    description: str
    shouting: bool = False  # a LOW P1 written as if the shop were on fire


def _high(action: str, subject: str, description: str) -> Template:
    return Template("P1", HIGH, action, subject, description)


def _low(priority: str, subject: str, description: str, shouting: bool = False) -> Template:
    return Template(priority, LOW, None, subject, description, shouting)


TEMPLATES: tuple[Template, ...] = (
    # --- P1, really HIGH, ROLLBACK: a release, update, deploy or new version ---------------
    _high(
        ROLLBACK,
        "Orders failing since this morning's update",
        "Since the new checkout version went live at {time}, every order fails at the confirm "
        "step with {error_code}. Our whole queue is customers who cannot buy.",
    ),
    _high(
        ROLLBACK,
        "Blank checkout page after the release",
        "Since the release at {time}, the checkout page goes blank after the address step for "
        "everyone we have asked. {count} customers have called in the last half hour and none of "
        "them could place an order.",
    ),
    _high(
        ROLLBACK,
        "Cart empties at checkout since the deploy",
        "Since your deploy at {time}, the cart empties itself when customers press Continue to "
        "checkout. It happens to every customer on every device, so no orders are coming through.",
    ),
    _high(
        ROLLBACK,
        "Place order button greyed out since the new version",
        "Since the new checkout version was rolled out at {time}, the Place order button stays "
        "greyed out for all customers, on the website and in the app. Nobody has finished an "
        "order since.",
    ),
    # --- P1, really HIGH, SCALE_OUT: a sale, campaign, peak or rush, and slowness ----------
    _high(
        SCALE_OUT,
        "Checkout times out during the flash sale",
        "Since the sale started at {time}, checkout spins for a minute and then times out, for "
        "every customer we have talked to.",
    ),
    _high(
        SCALE_OUT,
        "Checkout crawling since the campaign email went out",
        "Since the campaign email went out at {time}, checkout takes over a minute to load for "
        "all customers, and most of them give up before the order goes through. We have {count} "
        "complaints about the slowness already.",
    ),
    _high(
        SCALE_OUT,
        "Black Friday rush: checkout timing out",
        "Since the Black Friday rush began at {time}, the checkout page loads for ages and then "
        "fails with a timeout for all customers. It was fine this morning, before the traffic "
        "picked up.",
    ),
    _high(
        SCALE_OUT,
        "Confirm order spins forever at peak time",
        "Since the evening peak started at {time}, pressing Confirm order just spins until it "
        "times out, for everyone on the site. The shop is busier than ever and nobody can check "
        "out.",
    ),
    # --- P1, really HIGH, INVESTIGATE_UPSTREAM: the payment provider or gateway ------------
    _high(
        INVESTIGATE_UPSTREAM,
        "Every card payment declined",
        "Since {time} all {card_brand} and other card payments fail with gateway error "
        "{error_code} from our payment provider. Nobody can pay.",
    ),
    _high(
        INVESTIGATE_UPSTREAM,
        "Payment provider rejecting every payment",
        "Since {time}, every payment attempt comes back from the payment provider with error "
        "{error_code}, whatever the card. Customers reach the last step and cannot pay.",
    ),
    _high(
        INVESTIGATE_UPSTREAM,
        "Card check from the payment gateway failing for everyone",
        "Since {time}, the card verification page from the payment gateway shows {error_code} "
        "and sends every customer back to the cart. Not a single card payment has gone through "
        "since then.",
    ),
    _high(
        INVESTIGATE_UPSTREAM,
        "All payments failing at the payment gateway",
        "Since {time}, the payment gateway answers every authorisation with {error_code}, so all "
        "customers are told their payment could not be processed. Our phones have not stopped "
        "ringing.",
    ),
    # --- P1, really LOW, shouting -------------------------------------------------------
    _low(
        "P1",
        "URGENT!!! DISCOUNT CODE NOT WORKING",
        "My code {discount_code} does not apply on order {order_id}. I am losing money on every "
        "minute of this, fix it NOW.",
        shouting=True,
    ),
    _low(
        "P1",
        "CHECKOUT IS BROKEN!!! FIX IT NOW",
        "I have tried three times to buy the {product} and the page keeps asking me to log in "
        "again. This is UNACCEPTABLE, I want a manager to call me TODAY.",
        shouting=True,
    ),
    _low(
        "P1",
        "URGENT: CHARGED TWICE!!!",
        "I was charged {amount} twice for order {order_id}!!! I want my money back IMMEDIATELY "
        "or I am going to my bank.",
        shouting=True,
    ),
    _low(
        "P1",
        "!!! WRONG DELIVERY DATE !!!",
        "Order {order_id} says delivery on Friday but I need the {product} for my trip on "
        "Thursday. I NEED AN ANSWER IN THE NEXT HOUR.",
        shouting=True,
    ),
    # --- P1, really LOW: one customer, a question, a request, something cosmetic ----------
    _low(
        "P1",
        "Saved cards not loading for me",
        "My saved cards do not show up at checkout, so I have to type my card number in every "
        "time. Is something wrong with my account?",
    ),
    _low(
        "P1",
        "Cannot change the delivery address on my order",
        "I placed order {order_id} at {time} with my old address. Can you change it before it "
        "ships?",
    ),
    _low(
        "P1",
        "Logo looks blurry on the checkout page",
        "On {browser} the River Shop logo at the top of checkout looks blurry and slightly "
        "stretched. Not a big deal, but it looks unprofessional.",
    ),
    _low(
        "P1",
        "Can I split a payment between two cards?",
        "I want to pay {amount} for the {product}, half on each of my two cards. Is that possible "
        "at checkout?",
    ),
    _low(
        "P1",
        "Please add Apple Pay to checkout",
        "Most shops I buy from let me pay with Apple Pay. Could you add it to your checkout?",
    ),
    _low(
        "P1",
        "Typo on the order confirmation page",
        "The confirmation page says Thank you for you order instead of your order. Small thing, "
        "but I thought you should know.",
    ),
    _low(
        "P1",
        "Promo code rejected for one item",
        "The code {discount_code} works for everything in my basket except the {product}. Is that "
        "item excluded from the offer?",
    ),
    _low(
        "P1",
        "Invoice needs our company VAT number",
        "I need an invoice for order {order_id} with our company VAT number on it. Could you send "
        "me a corrected one?",
    ),
    # --- P2, LOW ------------------------------------------------------------------------
    _low(
        "P2",
        "Promo code rejected at checkout",
        "The code {discount_code} from your newsletter says it is no longer valid, but the email "
        "says it runs until Sunday. Can you check?",
    ),
    _low(
        "P2",
        "Cart total not updating after quantity change",
        "When I change the quantity of the {product} in my cart, the total only updates after I "
        "reload the page.",
    ),
    _low(
        "P2",
        "Address validation failing for my street",
        "Checkout keeps rejecting my address in {country} as invalid, although it is correct. I "
        "had to use my office address instead.",
    ),
    _low(
        "P2",
        "Tax looks wrong for my country",
        "Checkout added {amount} of VAT to order {order_id} to {country}, which looks too high. "
        "Could someone double-check the rate?",
    ),
    _low(
        "P2",
        "Duplicate order created",
        "I clicked Place order once but got two confirmation emails, for {order_id} and the order "
        "after it. Please cancel the second one.",
    ),
    _low(
        "P2",
        "Gift message missing from my order",
        "I added a gift message to order {order_id}, but it does not show on the order summary. "
        "Will it still be printed?",
    ),
    _low(
        "P2",
        "Delivery estimate does not match the product page",
        "Checkout says the {product} arrives in 10 days, but the product page says 2 to 3 days. "
        "Which one is right?",
    ),
    _low(
        "P2",
        "Cannot remove an item from my cart on mobile",
        "On {browser}, the remove button next to the {product} in my cart does nothing. I can only "
        "set the quantity to zero.",
    ),
    # --- P3, LOW ------------------------------------------------------------------------
    _low(
        "P3",
        "Order confirmation email delayed",
        "I placed order {order_id} an hour ago and the confirmation email has not arrived yet.",
    ),
    _low(
        "P3",
        "Shipping options missing for my address",
        "Only standard shipping is offered for my address in {country}. Is express delivery not "
        "available there?",
    ),
    _low(
        "P3",
        "Request: save my basket for later",
        "It would be nice to save my basket and come back to it next week. Is that something you "
        "plan to add?",
    ),
    _low(
        "P3",
        "Font on the receipt is very small",
        "The receipt PDF for order {order_id} uses a tiny font that is hard to read when printed.",
    ),
    _low(
        "P3",
        "How do I use a gift card?",
        "I got a River Shop gift card for my birthday. Where do I enter it at checkout?",
    ),
    _low(
        "P3",
        "Newsletter box ticked by default",
        "The newsletter box at checkout was already ticked when I got there. I would prefer it "
        "unticked.",
    ),
    _low(
        "P3",
        "Prices flash a dollar sign for a second",
        "When the checkout page loads on {browser}, prices show a dollar sign for a moment before "
        "they switch to euros.",
    ),
    _low(
        "P3",
        "Checkout was quick, thank you",
        "Just wanted to say that checkout was quick and easy on my last order, {order_id}. Keep it "
        "up.",
    ),
)

# The values a placeholder can take. Amounts are in euros, like the shop.
POOLS: dict[str, tuple[str, ...]] = {
    "time": ("08:40", "09:15", "10:05", "11:30", "12:50", "14:10", "15:45", "17:20", "18:35", "20:05"),
    "order_id": ("RS-481207", "RS-482913", "RS-483350", "RS-484018", "RS-485562", "RS-486741", "RS-487095"),
    "amount": ("€24.90", "€58.00", "€84.90", "€129.00", "€212.50", "€349.99"),
    "card_brand": ("Visa", "Mastercard", "American Express"),
    "error_code": ("E1042", "E2210", "E3307", "E4019", "E5003", "E6120"),
    "count": ("30", "45", "60", "80", "120"),
    "discount_code": ("SPRING10", "WELCOME15", "RIVER20", "FREESHIP"),
    "product": ("rain jacket", "trail shoes", "dry bag", "kayak paddle", "camping stove", "head torch"),
    "country": ("Austria", "Belgium", "Denmark", "Ireland", "Portugal"),
    "browser": ("Safari on an iPhone", "Firefox", "Chrome on Android", "Edge"),
}


def placeholders(template: Template) -> list[str]:
    """The placeholder names in a template's subject and description, sorted."""
    names = {
        name
        for text in (template.subject, template.description)
        for _, name, _, _ in string.Formatter().parse(text)
        if name is not None
    }
    return sorted(names)


def fill(template: Template, rng: random.Random) -> tuple[str, str]:
    """The template's subject and description, each placeholder drawn once from its pool (so
    a name used in both reads the same in both). Draws in sorted name order, so a seeded rng
    gives the same text."""
    values = {name: rng.choice(POOLS[name]) for name in placeholders(template)}
    return template.subject.format(**values), template.description.format(**values)
