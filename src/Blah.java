public class Blah {
    public static void main(String[] args) {
        double Cattles = 165.0;
        double clay_liner = 1.0;
        double trenching = 0.5;
        double Distance = 200.0;
        double wire_fence = 1.0;
        double plastic_liner = 0.0;
        double sqrtCattles = Math.sqrt(Cattles);
        double temp3 = 2.232 * Cattles + 11.338 * sqrtCattles;
        double temp = 3.72 * Cattles + trenching * 7.94 * sqrtCattles + 0.844 * Distance +
        clay_liner * temp3;

        double temp2 = (0.5 * 9.5 + 7.47) * temp3;

        double max = 1.38e-10 * Math.pow(temp,2.0)
        - 5.027e-5 * temp
        + 6.736 + clay_liner * temp2
        + plastic_liner / 0.7 * temp2
        + wire_fence * (189.0 + Math.sqrt(820.0 * Cattles))
        + 10000.0;
        max *= 1.1483;

        temp3 = 1.512 * Cattles + 9.332 * sqrtCattles;
        temp = 2.52 * Cattles + trenching * 6.54 * sqrtCattles + 0.844 * Distance +
        clay_liner * temp3;
        temp2 = (0.5 * 9.5 + 7.47) * temp3;
        double min = 1.38e-10 * Math.pow(temp, 2.0)
        - 5.027e-5 * temp
        + 6.736 + clay_liner * temp2
        + plastic_liner / 0.7 * temp2
        + wire_fence * (189.0 + Math.sqrt(556.0 * Cattles))
        + 10000.0;
        min *= 1.1483;

        double result = min / 2.0 + max / 2.0;
        double annual = result / 12;
        System.out.printf("%.10f", result);        
        System.out.println(": THIS IS THE ANSWER YOU NEED TO GET");
        System.out.printf("%.10f", annual);
        System.out.println(": ANNUAL COST");
    }
}
