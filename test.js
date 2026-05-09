try { ( manualError ); } catch(e) { console.log("Error in expression 0:", e); }
try { ( clientName(c) ); } catch(e) { console.log("Error in expression 1:", e); }
try { ( d.nom || d.id ); } catch(e) { console.log("Error in expression 2:", e); }
try { ( d.montant_ttc ? Number(d.montant_ttc).toFixed(2) + ' €' :
                                    '-' ); } catch(e) { console.log("Error in expression 3:", e); }
try { ( editError ); } catch(e) { console.log("Error in expression 4:", e); }
try { ( clientName(c) ); } catch(e) { console.log("Error in expression 5:", e); }
try { ( d.nom || d.id ); } catch(e) { console.log("Error in expression 6:", e); }
try { ( d.montant_ttc ? Number(d.montant_ttc).toFixed(2) + ' €' :
                                    '-' ); } catch(e) { console.log("Error in expression 7:", e); }
try { ( showTerminees ? 'Masquer terminées' : 'Afficher terminées'
                                ); } catch(e) { console.log("Error in expression 8:", e); }
try { ( cmd.reference || '-' ); } catch(e) { console.log("Error in expression 9:", e); }
try { ( cmd.priorite || 'normale' ); } catch(e) { console.log("Error in expression 10:", e); }
try { ( clientName(cmd.contact) ); } catch(e) { console.log("Error in expression 11:", e); }
try { ( cmd.montant_ttc ? Number(cmd.montant_ttc).toFixed(2) +
                                        ' €' : '-' ); } catch(e) { console.log("Error in expression 12:", e); }
try { ( formatDate(cmd.date_commande) ); } catch(e) { console.log("Error in expression 13:", e); }
try { ( selectedCommande.reference ); } catch(e) { console.log("Error in expression 14:", e); }
try { ( clientName(selectedCommande.contact) ); } catch(e) { console.log("Error in expression 15:", e); }
try { ( selectedCommande.montant_ttc ?
                                    Number(selectedCommande.montant_ttc).toFixed(2) + ' € TTC' :
                                    '-' ); } catch(e) { console.log("Error in expression 16:", e); }
try { ( selectedCommande.description ); } catch(e) { console.log("Error in expression 17:", e); }
try { ( formatDate(selectedCommande.date_livraison_prevue) || `Non définie` ); } catch(e) { console.log("Error in expression 18:", e); }
try { ( selectedCommande.notes_internes || 'Aucune note interne.'
                                ); } catch(e) { console.log("Error in expression 19:", e); }
try { ( stats.en_attente ); } catch(e) { console.log("Error in expression 20:", e); }
try { ( stats.en_cours ); } catch(e) { console.log("Error in expression 21:", e); }
try { ( stats.terminee ); } catch(e) { console.log("Error in expression 22:", e); }
try { ( stats.urgente ); } catch(e) { console.log("Error in expression 23:", e); }
try { ( stats.haute ); } catch(e) { console.log("Error in expression 24:", e); }
try { ( stats.normale ); } catch(e) { console.log("Error in expression 25:", e); }
try { ( stats.basse ); } catch(e) { console.log("Error in expression 26:", e); }
