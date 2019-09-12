package com.Leo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CreatingDatasets {

    private Random random = new Random();

    private int maxCus;
    private int maxTrans;

    CreatingDatasets(int maxCus, int maxTrans) {
        this.maxCus = maxCus;
        this.maxTrans = maxTrans;
    }

    public void creatingDatasets() throws IOException {
        addCustomer(maxCus);
        addTransaction(maxTrans);
    }

    private int randomNumber(int max, int min){
        return random.nextInt(max) % (max - min + 1) + min;
    }

    private String randomCharacter(int number){
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < number; i++) {
            s.append((char) (random.nextInt(26) + 'a'));
        }
        return s.toString();
    }

    private String newCustomer(int ID) {
        String name = randomCharacter(randomNumber(20,10));
        name = name.substring(0, 1).toUpperCase() + name.substring(1);
        int age = randomNumber(70, 10);
        int genderNumber = randomNumber(1, 0);
        String gender;
        if(genderNumber == 0){
            gender = "Male";
        }
        else {
            gender = "Female";
        }
        int countryCode = randomNumber(10, 1);
        float salary = random.nextFloat() * 9900 + 100;
        List<String> list = new ArrayList<>();
        list.add(ID + "");
        list.add(name);
        list.add(age + "");
        list.add(gender);
        list.add(countryCode + "");
        list.add(String.format("%.2f", salary));
        return String.join(",", list);
    }

    private String newTransaction(int ID) {
        int custID = randomNumber(50_000, 1);
        float transTotal = random.nextFloat() * 990 + 10;
        int transNumItems = randomNumber(10,1);
        String transDesc = randomCharacter(randomNumber(50,20));
        List<String> list = new ArrayList<>();
        list.add(ID + "");
        list.add(custID + "");
        list.add(String.format("%.2f", transTotal));
        list.add(transNumItems + "");
        list.add(transDesc);
        return String.join(",", list);
    }

    private void addCustomer(int maxID) throws IOException {
        File file = new File("Customers.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        for (int i = 0; i < maxID; i++) {
            writer.write(newCustomer(i + 1) + "\r\n");
        }
        writer.close();
    }

    private void addTransaction(int maxID) throws IOException {
        File file = new File("Transactions.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        for (int i = 0; i < maxID; i++) {
            writer.write(newTransaction(i + 1) + "\r\n");
        }
        writer.close();
    }

}


