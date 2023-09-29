package br.com.fiap.domain.service;

import br.com.fiap.domain.entity.Instrutor;
import br.com.fiap.domain.repository.InstrutorRepository;
import br.com.fiap.exception.MetodoNaoImplementadoException;

import java.util.List;

public class InstrutorService implements Service<Instrutor, Long> {

    private InstrutorRepository repository = InstrutorRepository.build();
    @Override
    public List<Instrutor> findAll() {
        return repository.findAll();
    }

    @Override
    public Instrutor findById(Long id) {
        return repository.findById(id);
    }

    @Override
    public List<Instrutor> findByName(String texto) {
        return repository.findByName(texto);
    }

    @Override
    public Instrutor persist(Instrutor instrutor) {
        return repository.persist(instrutor);
    }
}