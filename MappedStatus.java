
enum MappedStatus {
    MAPPED("Mapped"),

    UNMAPPED("Unmapped"),

    MISSING_IN_HYBRIS("Missing in Hybris"),

    MISSING_IN_SCPLUS("Missing in SCplus");

    private final String value;

    MappedStatus(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}