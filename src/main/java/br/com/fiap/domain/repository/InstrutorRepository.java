package br.com.fiap.domain.repository;


import br.com.fiap.domain.entity.Instrutor;
import br.com.fiap.domain.infra.ConnectionFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class InstrutorRepository implements Repository<Instrutor,Long> {

    private static AtomicReference<InstrutorRepository> instance = new AtomicReference<>();

    private InstrutorRepository() {
    }

    public static InstrutorRepository build() {
        InstrutorRepository result = instance.get();
        if (Objects.isNull(result)) {
            InstrutorRepository repository = new InstrutorRepository();
            if (instance.compareAndSet(null, repository)) {
                result = repository;
            } else {
                result = instance.get();
            }
        }
        return result;
    }

    @Override
    public List<Instrutor> findAll() {
        List<Instrutor> instrutores = new ArrayList<>();

        try {
            var factory = ConnectionFactory.build();
            Connection connection = factory.getConnection();

            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM instrutor");

            if (rs.isBeforeFirst()) {
                while (rs.next()) {
                    Long id = rs.getLong("INSTRUTOR_ID");
                    String nome = rs.getString("NOME");

                    instrutores.add(new Instrutor(id, nome));
                }
            }
            st.close();
            rs.close();
            connection.close();

        } catch (SQLException e) {
            System.err.println("Não foi possivel consultar os dados!\n" + e.getMessage());
        }
        return instrutores;
    }

    @Override
    public Instrutor findById(Long id) {
        Instrutor instrutor = null;
        var sql = "SELECT * FROM INSTRUTOR WHERE INSTRUTOR_ID=?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();

            if (rs.isBeforeFirst()) {
                while (rs.next()) {
                    var id_instrutor = rs.getLong("INSTRUTOR_ID");
                    var nome = rs.getString("NOME");
                    instrutor = new Instrutor(id_instrutor, nome);
                }
            } else {
                System.out.println("Instrutor não encontrado com o id = " + id);
            }
            rs.close();
            ps.close();
            connection.close();
        } catch (SQLException e) {
            System.err.println("Não foi possível executar a consulta: \n" + e.getMessage());
        }
        return instrutor;
    }

    @Override
    public List<Instrutor> findByName(String texto) {
        List<Instrutor> instrutores = new ArrayList<>();
        var sql = "SELECT * FROM INSTRUTOR where UPPER(NOME) like ?";

        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            texto = Objects.nonNull(texto) ? texto.toUpperCase() : "";
            ps.setString(1, "%" + texto + "%");
            ResultSet rs = ps.executeQuery();
            if (rs.isBeforeFirst()) {
                while (rs.next()) {
                    Long id = rs.getLong("INSTRUTOR_ID");
                    String nome = rs.getString("NOME");
                    instrutores.add(new Instrutor(id, nome));
                }
            } else {
                System.out.println("Instrutor não encontrado com o nome = " + texto);
            }
            ps.close();
            rs.close();
            connection.close();
        } catch (SQLException e) {
            System.err.println("Não foi possível executar a consulta: \n" + e.getMessage());
        }
        return instrutores;
    }

    @Override
    public Instrutor persist(Instrutor instrutor) {
        var sql = "BEGIN" +
                " INSERT INTO instrutor (NOME) " +
                "VALUES (?) " +
                "returning INSTRUTOR_ID into ?; " +
                "END;" +
                "";
        var factory = ConnectionFactory.build();
        Connection connection = factory.getConnection();

        CallableStatement cs = null;
        try {
            cs = connection.prepareCall(sql);
            cs.setString(1, instrutor.getNome());
            cs.registerOutParameter(2, Types.BIGINT);
            cs.executeUpdate();
            instrutor.setId(cs.getLong(2));
            cs.close();
            connection.close();
        } catch (SQLException e) {
            System.err.println("Não foi possível executar o comando!\n" + e.getMessage());
        }
        return instrutor;
    }
}