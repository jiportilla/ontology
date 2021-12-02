Given this input:<br />
`This specialty applies their project management skills supporting Identity & Access Management projects.`

Generate:
```
------  specialty
[('specialty', 1)]
	 ('specialty applies', 1)
		 ('specialty applies management', 1)


------  applies
[('applies', 1)]
	 ('applies management', 1)
		 ('applies management supporting', 1)
		 ('specialty applies management', 1)
	 ('specialty applies', 1)
		 ('specialty applies management', 1)


------  management
[('management', 1)]
	 ('applies management', 1)
		 ('applies management supporting', 1)
		 ('specialty applies management', 1)
	 ('management supporting', 1)
		 ('applies management supporting', 1)
		 ('management supporting Identity', 1)


------  supporting
[('supporting', 1)]
	 ('management supporting', 1)
		 ('applies management supporting', 1)
		 ('management supporting Identity', 1)
	 ('supporting Identity', 1)
		 ('management supporting Identity', 1)
		 ('supporting Identity Access', 1)


------  Identity
[('Identity', 1)]
	 ('Identity Access', 1)
		 ('Identity Access Management', 1)
		 ('supporting Identity Access', 1)
	 ('supporting Identity', 1)
		 ('management supporting Identity', 1)
		 ('supporting Identity Access', 1)


------  Access
[('Access', 1)]
	 ('Access Management', 1)
		 ('Identity Access Management', 1)
	 ('Identity Access', 1)
		 ('Identity Access Management', 1)
		 ('supporting Identity Access', 1)


------  Management
[('Management', 1)]
	 ('Access Management', 1)
		 ('Identity Access Management', 1)
```

This quick and dirty tool makes it easy to spot candidate bigrams and trigrams for adding to the Ontology.