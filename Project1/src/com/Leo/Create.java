package com.Leo;

import java.io.IOException;

public class Create {
    public static void main(String[] args) throws IOException {
        CreatingDatasets cd = new CreatingDatasets(5000, 500000);
        cd.creatingDatasets();
    }

}
