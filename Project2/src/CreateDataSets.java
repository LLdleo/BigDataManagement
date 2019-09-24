import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateDataSets {
    Random random = new Random(1);
    int maxPoints;
    int maxRectangles;

    CreateDataSets(int maxPoints, int maxRectangles) {
        this.maxPoints = maxPoints;
        this.maxRectangles = maxRectangles;
    }

    void createDatasets() throws IOException {
        addPoint();
        addRectangle();
    }

    int randomNumber(int max, int min) {
        return random.nextInt(max) % (max - min + 1) + min;
    }

    String newPoint(int num) {
        int coordinate1 = randomNumber(10000, 1);
        int coordinate2 = randomNumber(10000, 1);
        return coordinate1 + "," + coordinate2;
    }

    String newRectangle(int num) {
        int coordinate1 = randomNumber(10000, 1);
        int coordinate2 = randomNumber(10000, 1);
        int height = randomNumber(20, 1);
        int width = randomNumber(5, 1);
        return "R" + num + "," + coordinate1 + "," + coordinate2 + "," + height + "," + width;
    }

    void addPoint() throws IOException {
        File file = new File("Points_test");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        for (int i = 0; i < maxPoints; i++) {
            writer.write(newPoint(i + 1) + "\r\n");
        }
        writer.close();
    }

    void addRectangle() throws IOException {
        File file = new File("Rectangles_test");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        for (int i = 0; i < maxRectangles; i++) {
            writer.write(newRectangle(i + 1) + "\r\n");
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        CreateDataSets cd = new CreateDataSets(10000, 1000);
        cd.createDatasets();
    }
}
