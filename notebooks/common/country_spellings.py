"""Country spellings seen in the source systems, shared by the customer country pipeline
(notebooks 31 and 32). One list per real country: every way the CRM and the order system
have written it. The pipeline passes these through as received today."""

SPELLINGS = [
    ["USA", "U.S.A.", "United States", "united states of america", " us ", "America"],
    ["Canada", "CA", "canada "],
    ["Mexico", "MEX", "mexico"],
    ["Brazil", "Brasil", "BR"],
    ["Argentina", "argentina", "AR"],
    ["UK", "United Kingdom", "Great Britain", "England"],
    ["Ireland", "Republic of Ireland", "IE"],
    ["France", "FR", "france"],
    ["Germany", "Deutschland", "DE"],
    ["Spain", "España", "ES"],
    ["Italy", "Italia", "IT"],
    ["Netherlands", "Holland", "The Netherlands"],
    ["Switzerland", "Schweiz", "CH"],
    ["Sweden", "sweden", "SE"],
    ["India", "IND", "india", "Bharat"],
    ["China", "PRC", "People's Republic of China"],
    ["Japan", "JP", "japan"],
    ["South Korea", "Korea, Republic of", "Republic of Korea"],
    ["Singapore", "SG", "singapore"],
    ["UAE", "United Arab Emirates", "U.A.E."],
    ["Saudi Arabia", "KSA", "saudi arabia"],
    ["South Africa", "RSA", "south africa"],
    ["Nigeria", "nigeria", "NG"],
    ["Egypt", "egypt", "EG"],
    ["Australia", "AU", "Aus"],
    ["New Zealand", "NZ", "new zealand"],
]
