# Create list of departement zip codes
all_deps = [
    *"-7510".join(list(str(x) for x in range(0, 10))).split("-")[1:],
    *"-751".join(list(str(x) for x in range(9, 21))).split("-")[1:],
    *["971", "972", "973", "974", "975", "976", "977", "978", "98"],
    *"-0".join(list(str(x) for x in range(0, 10))).split("-")[1:],
    *list(str(x) for x in range(10, 20)),
    *["2A", "2B"],
    *list(str(x) for x in range(21, 96)),
    *[""],
]
# Remove Paris zip code
all_deps.remove("75")
