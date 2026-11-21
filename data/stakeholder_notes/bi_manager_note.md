# BI Manager note: semantic layer ground rules for the funnel model

To: Platform delivery (whoever picks this up)
Re: Marketing vs Ops conversion definitions

I've now got a brief from the Regional Marketing Director (UK & Ireland)
and a brief from the Sales Operations Director (International), and they
do not define the funnel the same way. I've read both. I'm not going to
referee whose definition is "right" in this note - that's a conversation
for the two of them, and I'd rather it happened properly than have it
decided by default inside a data model neither of them reviewed.

What I do own is the semantic layer, and here is what has to be true of it
regardless of how that conversation ends:

- **One certified measure per KPI.** Whatever we ship, each KPI has exactly
  one certified definition in the semantic layer. I don't want two measures
  quietly answering the same business question with different numbers and
  no way to tell them apart.

- **If two grains are genuinely needed, name both and document both.** I'm
  not ruling out that Marketing and Ops need different things - a lead-grain
  number and a call-grain number might both be legitimate. If that's the
  outcome, both measures get their own name, their own description, and
  their own row in the KPI definitions doc. Neither one gets to quietly
  stand in for the other.

- **Nothing gets called "Conversion Rate" on its own.** Every measure name
  that reports a conversion has to carry a qualifier that says what it's
  a conversion of and at what grain - so whoever is looking at the report
  can tell which definition they're looking at without having to ask.

Bring me the model before it goes near a dashboard. I'd rather catch a
naming problem in review than explain to two directors why the same word
means two different numbers in the same report.
