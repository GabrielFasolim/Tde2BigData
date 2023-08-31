package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FireAvgTempWritable
        implements WritableComparable<FireAvgTempWritable> {
    //Atributos Privados
    private float soma;
    private int n;

    // Construtor vazio


    public FireAvgTempWritable() {
    }
    //Demais construtores sao opcionais
    public FireAvgTempWritable(float soma, int n) {
        this.soma = soma;
        this.n = n;
    }

    //get e sets


    public float getSoma() {
        return soma;
    }

    public void setSoma(float soma) {
        this.soma = soma;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    //equals e hashcode criar apenas dando next :)

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireAvgTempWritable that = (FireAvgTempWritable) o;
        return Float.compare(soma, that.soma) == 0 && n == that.n;
    }

    @Override
    public int hashCode() {
        return Objects.hash(soma, n);
    }

    //compare to

    @Override
    public int compareTo(FireAvgTempWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(soma);
        dataOutput.writeInt(n);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //leitura de dados precisa seguir a ordem do write
        soma = dataInput.readFloat();
        n = dataInput.readInt();

    }
}
