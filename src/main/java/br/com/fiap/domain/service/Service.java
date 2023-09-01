package br.com.fiap.domain.service;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public interface Service<T, U> {

    public List<T> findAll();

    public T findById(U id);

    public List<T> findByName(String texto);

    public T persist(T t);

    default boolean valido(String s) {

        return Objects.nonNull( s ) && !s.trim().isEmpty();
    }

    /**
     * Validando email
     *
     * @param emailAddress
     * @return
     */
    default boolean validarEmail(String emailAddress) {
        //Expressão Regular. Para saber mais:
        //Dica de leitura: Expressões Regulares: Uma Abordagem Divertida
        //ISBN 13: 978-8575224748
        var regexPattern = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$";
        return Pattern.compile( regexPattern )
                .matcher( emailAddress )
                .matches();
    }
}