public class Driver {
    public static void main(String[] args) throws Exception {
        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        Normalize normalize = new Normalize();
        Multiplication multiplication = new Multiplication();
        Sum sum = new Sum();

        String rawInputDir = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixDir = args[2];
        String normalizeDir = args[3];
        String multiplicationDir = args[4];
        String sumDir = args[5];

        dataDividerByUser.main(new String[]{rawInputDir, userMovieListOutputDir});
        coOccurrenceMatrixGenerator.main(new String[]{userMovieListOutputDir, coOccurrenceMatrixDir});
        normalize.main(new String[]{coOccurrenceMatrixDir, normalizeDir});
        multiplication.main(new String[]{normalizeDir, rawInputDir, multiplicationDir});
        sum.main(new String[]{multiplicationDir, sumDir});

    }
}
