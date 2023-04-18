public class VariantProduct {
    Product product;
    private String baseProduct;

    public VariantProduct(Product product, String baseProduct) {
        this.product = product;
        this.baseProduct = baseProduct;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    public VariantProduct() {

    }
    public VariantProduct(String baseProduct) {
        this.baseProduct = baseProduct;
    }

    public VariantProduct(String code, String name, String baseProduct) {
        super(code, name);
        this.baseProduct = baseProduct;
    }

    public String getBaseProduct() {
        return baseProduct;
    }

    public void setBaseProduct(String baseProduct) {
        this.baseProduct = baseProduct;
    }
}
