package deduplication.gui.component.renderer;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JButton;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

public class JButtonRenderer extends JButton implements TableCellRenderer {
	
	private static final long serialVersionUID = -6725960826572276067L;
	private Color background;

	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean selected, boolean hasFocus, int row, int column) {				
		if(selected) {
			background = table.getSelectionBackground();
		} else {
			background = table.getBackground();
		}			
		return this;
	}
	
	/*public void paint(Graphics graphics) {
		super.paint(graphics);
		if(!this.isEnabled()) {
			graphics.setColor(background);
			graphics.fillRect(0, 0, this.getWidth(), this.getHeight());
		}
	}*/
	
}
